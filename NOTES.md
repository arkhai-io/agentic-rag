MarkerPDFToDocument (marker_pdf_converter.py:22-252) uses GPU/CUDA for multiple deep learning models:

  1. Layout Detection

  - Model: Custom LayoutLMv3 (Vision Transformer-based)
  - Purpose: Detects page structure elements (tables, diagrams, titles, captions, headers, footers)
  - GPU Usage: ~1-2GB VRAM

  2. Column Detection

  - Model: Another custom LayoutLMv3 model
  - Purpose: Identifies multi-column layouts and determines reading order
  - GPU Usage: ~500MB-1GB VRAM

  3. Text Recognition (OCR)

  - Model: Custom Donut model based on Swin Transformer (encoder-decoder architecture)
  - Purpose: Extracts text from PDF pages
  - Default OCR Engine: Surya OCR (more accurate but slower on CPU)
  - GPU Usage: ~1-2GB VRAM

  4. Equation Handling

  - Model: Nougat (transformer-based)
  - Purpose: Converts equation images to LaTeX code
  - GPU Usage: ~500MB-1GB VRAM







Variable Costs (Per Document):

  | Document Characteristic | Memory Impact           | Why                                                            |
  |-------------------------|-------------------------|----------------------------------------------------------------|
  | Number of pages         | +50-200MB per page      | Page images loaded into GPU for inference                      |
  | Image resolution/DPI    | +100-500MB for high-DPI | Larger tensors for vision transformers                         |
  | Tables                  | +200-500MB              | Table recognition model activation + table tensor processing   |
  | Equations               | +100-300MB              | Nougat model not in base dict, but loads if equations detected |
  | Complex layouts         | +100-200MB              | More layout segmentation passes                                |
  | OCR requirements        | +200-400MB per page     | Full Surya OCR pipeline activation                             |


---

## Batch Processing Analysis for MarkerPDFToDocument

### Current State
The `MarkerPDFToDocument` component processes PDFs sequentially (one-by-one loop) even when receiving a list of sources. The underlying Marker library's `PdfConverter.__call__()` API only accepts single file paths.

### Effort to Add True GPU Batch Processing

**Estimated effort: MEDIUM to HIGH** (not trivial)

#### Option 1: Use Marker's CLI-based batch processing
**Complexity**: Medium
- Marker provides `--workers` flag for parallel processing: `marker input_folder output_folder --workers 10`
- Implementation requires:
  1. Write all PDFs to temporary folder
  2. Call Marker CLI via subprocess with workers flag
  3. Read back converted markdown files
  4. Map results back to original ByteStream sources

**Issues**:
- Loses in-process Python API benefits
- Requires file I/O overhead (write all inputs, read all outputs)
- No programmatic control over GPU memory
- Shell dependency

#### Option 2: Implement custom batching with multiprocessing (based on Marker's internal convert.py)
**Complexity**: High
- Marker's CLI uses multiprocessing internally (marker/scripts/convert.py)
- Architecture:
  1. `multiprocessing.Pool` with worker_init to load models in each process
  2. Global `model_refs` in each worker (models loaded once per worker)
  3. Each worker processes PDFs using `process_single_pdf(args)`
  4. Uses `pool.imap_unordered()` for efficiency
- Would require extracting/replicating this logic from CLI code into reusable function

**Issues**:
- Logic is embedded in Click CLI decorators (marker/scripts/convert.py)
- No exposed programmatic API - would need to extract and refactor
- Each worker loads models independently (~5GB VRAM peak, ~3.5GB average per worker)
- Complex state management (models stored as process-local globals)
- Requires careful GPU memory management based on worker count

#### Option 3: Wait for Marker library enhancement
**Complexity**: None (external dependency)
- Marker developers may add native batch API in future
- Current v1.0+ doesn't expose batch processing in Python API
- CLI batch processing uses shell scripts, not exposed Python functions

### Recommendation for initial prototype
**Keep current sequential implementation** because:
1. Mock execution mode doesn't benefit from real batching
2. Simulated delays (1s/page) work identically whether sequential or parallel
3. Avoids complexity that doesn't serve initial prototype goals
4. Future refactor possible when real execution needed

### Production Considerations
For real production use (post-initial prototype):
- **If throughput critical**: Implement Option 2 with careful VRAM management
- **If simplicity preferred**: Use Option 1 with file-based batch CLI
- **If uncertain**: Monitor Marker library updates for native batch API

The embedder already has true GPU batching, so pipeline parallelism (converter on job N while embedder on job M) will still provide significant throughput gains.
