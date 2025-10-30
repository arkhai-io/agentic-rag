"""Tests for MarkItDown PDF converter component."""

import os
import tempfile
from pathlib import Path

import pytest

from agentic_rag.components.converters.markitdown_pdf_converter import (
    MarkItDownPDFToDocument,
)


class TestMarkItDownPDFToDocument:
    """Test MarkItDown PDF converter functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.converter = MarkItDownPDFToDocument()

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_markitdown_converter_initialization(self):
        """Test basic initialization of MarkItDown converter."""
        converter = MarkItDownPDFToDocument()
        assert converter.store_full_path is False
        assert converter._markitdown_instance is None

    def test_markitdown_converter_custom_initialization(self):
        """Test initialization with custom parameters."""
        converter = MarkItDownPDFToDocument(store_full_path=True)
        assert converter.store_full_path is True

    def test_markitdown_initialization_success(self):
        """Test successful MarkItDown initialization."""
        converter = MarkItDownPDFToDocument()

        try:
            converter._initialize_markitdown()
            assert converter._markitdown_instance is not None
            print("✅ MarkItDown initialization successful")
        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_markitdown_initialization_import_error(self):
        """Test MarkItDown initialization with missing dependency."""
        # This test assumes markitdown might not be installed
        converter = MarkItDownPDFToDocument()

        # Use unittest.mock for proper import mocking
        from unittest.mock import patch

        with patch("builtins.__import__") as mock_import:

            def side_effect(name, *args, **kwargs):
                if name == "markitdown":
                    raise ImportError("No module named 'markitdown'")
                return __import__(name, *args, **kwargs)

            mock_import.side_effect = side_effect

            with pytest.raises(
                ImportError, match="MarkItDown library is not installed"
            ):
                converter._initialize_markitdown()

    def test_convert_pdf_with_markitdown_file_not_found(self):
        """Test error handling when PDF file doesn't exist."""
        converter = MarkItDownPDFToDocument()

        try:
            converter._initialize_markitdown()

            non_existent_path = Path("/non/existent/file.pdf")
            with pytest.raises(FileNotFoundError):
                converter._convert_pdf_with_markitdown(non_existent_path)

        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_convert_pdf_with_markitdown_not_a_file(self):
        """Test error handling when path is not a file."""
        converter = MarkItDownPDFToDocument()

        try:
            converter._initialize_markitdown()

            # Use a directory instead of a file
            directory_path = Path(self.temp_dir)
            with pytest.raises(ValueError, match="Path is not a file"):
                converter._convert_pdf_with_markitdown(directory_path)

        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_convert_pdf_with_markitdown_not_pdf_file(self):
        """Test error handling when file is not a PDF."""
        converter = MarkItDownPDFToDocument()

        try:
            converter._initialize_markitdown()

            # Create a non-PDF file
            text_file = Path(self.temp_dir) / "test.txt"
            text_file.write_text("This is not a PDF")

            with pytest.raises(ValueError, match="File is not a PDF"):
                converter._convert_pdf_with_markitdown(text_file)

        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_convert_pdf_with_markitdown_success(self):
        """Test successful PDF conversion with MarkItDown."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create a simple PDF for testing
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_path = Path(self.temp_dir) / "test.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "Test PDF Content")
            c.drawString(100, 730, "This is a test document for MarkItDown conversion.")
            c.save()

            # Test conversion
            converter._initialize_markitdown()
            result = converter._convert_pdf_with_markitdown(pdf_path)

            assert isinstance(result, str)
            assert len(result) > 0
            print(
                f"✅ MarkItDown conversion successful: {len(result)} characters extracted"
            )

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_empty_sources(self):
        """Test run method with empty sources list."""
        converter = MarkItDownPDFToDocument()

        try:
            result = converter.run(sources=[])

            assert isinstance(result, dict)
            assert "documents" in result
            assert result["documents"] == []
            print("✅ Empty sources test passed")

        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_run_invalid_source(self):
        """Test run method with invalid source."""
        converter = MarkItDownPDFToDocument()

        try:
            # Try with non-existent file
            result = converter.run(sources=["/non/existent/file.pdf"])

            # Should return empty documents list (error handled gracefully)
            assert isinstance(result, dict)
            assert "documents" in result
            assert result["documents"] == []
            print("✅ Invalid source handling test passed")

        except ImportError:
            pytest.skip("MarkItDown not available")

    def test_run_successful_conversion(self):
        """Test successful end-to-end conversion."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create a test PDF
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_path = Path(self.temp_dir) / "test_doc.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "# Test Document")
            c.drawString(100, 730, "This is test content for MarkItDown.")
            c.drawString(100, 710, "## Section 1")
            c.drawString(100, 690, "Some detailed information here.")
            c.save()

            # Test conversion
            result = converter.run(sources=[str(pdf_path)])

            assert isinstance(result, dict)
            assert "documents" in result
            assert len(result["documents"]) == 1

            document = result["documents"][0]
            assert len(document.content) > 0
            assert document.meta["converter"] == "markitdown"
            assert "file_path" in document.meta

            print("✅ Successful conversion test passed")
            print(f"📄 Extracted {len(document.content)} characters")
            print(f"🏷️  Metadata: {document.meta}")
            print("📝 Converted Markdown Content:")
            print("=" * 50)
            print(document.content)
            print("=" * 50)

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_with_metadata(self):
        """Test conversion with custom metadata."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create a test PDF
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_path = Path(self.temp_dir) / "meta_test.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "Test document with metadata")
            c.save()

            # Test with custom metadata
            custom_meta = {"author": "Test Author", "category": "test"}
            result = converter.run(sources=[str(pdf_path)], meta=custom_meta)

            assert len(result["documents"]) == 1
            document = result["documents"][0]

            # Check that custom metadata was added
            assert document.meta["author"] == "Test Author"
            assert document.meta["category"] == "test"
            assert document.meta["converter"] == "markitdown"

            print("✅ Metadata handling test passed")
            print("📝 Converted Content with Metadata:")
            print("=" * 40)
            print(document.content)
            print("=" * 40)

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_store_full_path_false(self):
        """Test file path storage with store_full_path=False."""
        converter = MarkItDownPDFToDocument(store_full_path=False)

        try:
            # Create a test PDF in a subdirectory
            sub_dir = Path(self.temp_dir) / "subdir"
            sub_dir.mkdir()
            pdf_path = sub_dir / "test_file.pdf"

            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "Test content")
            c.save()

            result = converter.run(sources=[str(pdf_path)])
            document = result["documents"][0]

            # Should store only filename, not full path
            assert document.meta["file_path"] == "test_file.pdf"
            print("✅ store_full_path=False test passed")

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_store_full_path_true(self):
        """Test file path storage with store_full_path=True."""
        converter = MarkItDownPDFToDocument(store_full_path=True)

        try:
            # Create a test PDF
            pdf_path = Path(self.temp_dir) / "full_path_test.pdf"

            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "Test content")
            c.save()

            result = converter.run(sources=[str(pdf_path)])
            document = result["documents"][0]

            # Should store full path
            assert str(pdf_path) in document.meta["file_path"]
            print("✅ store_full_path=True test passed")

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_with_bytestream_input(self):
        """Test conversion with ByteStream input."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create a test PDF
            from haystack.dataclasses import ByteStream
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_path = Path(self.temp_dir) / "bytestream_test.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "ByteStream test content")
            c.save()

            # Read PDF as bytes and create ByteStream
            pdf_bytes = pdf_path.read_bytes()
            bytestream = ByteStream(
                data=pdf_bytes,
                meta={"file_path": "uploaded_document.pdf", "source": "upload"},
            )

            # Test conversion with ByteStream
            result = converter.run(sources=[bytestream])

            assert len(result["documents"]) == 1
            document = result["documents"][0]
            assert len(document.content) > 0
            assert document.meta["converter"] == "markitdown"
            assert document.meta["source"] == "upload"

            print("✅ ByteStream input test passed")
            print(f"📄 Extracted {len(document.content)} characters from ByteStream")
            print("📝 ByteStream Converted Content:")
            print("=" * 40)
            print(document.content)
            print("=" * 40)

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_multiple_sources(self):
        """Test conversion with multiple PDF sources."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create multiple test PDFs
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_files = []
            for i in range(3):
                pdf_path = Path(self.temp_dir) / f"multi_test_{i}.pdf"
                c = canvas.Canvas(str(pdf_path), pagesize=letter)
                c.drawString(100, 750, f"Document {i} content")
                c.drawString(100, 730, f"This is test document number {i}")
                c.save()
                pdf_files.append(str(pdf_path))

            # Test conversion of multiple files
            result = converter.run(sources=pdf_files)

            assert len(result["documents"]) == 3

            for i, document in enumerate(result["documents"]):
                assert len(document.content) > 0
                assert document.meta["converter"] == "markitdown"
                assert f"multi_test_{i}.pdf" in document.meta["file_path"]

            print(
                f"✅ Multiple sources test passed - converted {len(result['documents'])} PDFs"
            )

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_run_empty_content_warning(self):
        """Test handling of PDFs that produce empty content."""
        converter = MarkItDownPDFToDocument()

        try:
            # Create a minimal PDF with no readable text
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            pdf_path = Path(self.temp_dir) / "empty_content.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            # Don't add any text content
            c.save()

            result = converter.run(sources=[str(pdf_path)])

            # Should still create a document, even if content is empty
            assert len(result["documents"]) == 1
            document = result["documents"][0]
            assert document.meta["converter"] == "markitdown"

            print("✅ Empty content handling test passed")

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    @pytest.mark.skip(
        reason="Test uses outdated API (runner.load_pipeline), unrelated to ingestion server implementation"
    )
    def test_integration_with_pipeline(self):
        """Test MarkItDown converter integration in a pipeline."""
        try:
            from reportlab.lib.pagesizes import letter
            from reportlab.pdfgen import canvas

            from agentic_rag.pipeline import PipelineRunner

            # Create a test PDF
            pdf_path = Path(self.temp_dir) / "pipeline_test.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=letter)
            c.drawString(100, 750, "# Pipeline Integration Test")
            c.drawString(100, 730, "This PDF will be processed through the pipeline.")
            c.drawString(100, 710, "## Section 1")
            c.drawString(100, 690, "Content for testing pipeline integration.")
            c.save()

            # Create pipeline with MarkItDown converter
            runner = PipelineRunner()
            pipeline_spec = [
                {"type": "CONVERTER.MARKITDOWN_PDF"},
                {"type": "CHUNKER.MARKDOWN_AWARE"},
            ]

            config = {
                "markitdown_pdf_converter": {"store_full_path": True},
                "markdown_aware_chunker": {"chunk_size": 500, "chunk_overlap": 50},
            }

            runner.load_pipeline(pipeline_spec, "markitdown_test", config)

            # Note: This is a simplified test - in real usage, you'd handle file loading differently
            # For now, just verify the pipeline can be created with MarkItDown converter

            print("✅ Pipeline integration test setup completed")
            print("📝 Note: Full pipeline integration requires file loading components")

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")

    def test_component_available_in_registry(self):
        """Test that MarkItDown converter is available in component registry."""
        from agentic_rag import list_available_components

        available = list_available_components()

        assert "CONVERTER" in available
        assert "MARKITDOWN_PDF" in available["CONVERTER"]

        print("✅ MarkItDown converter found in registry")

    @pytest.mark.skip(
        reason="Test uses outdated API (factory.create_pipeline_from_spec), unrelated to ingestion server implementation"
    )
    def test_error_handling_with_markitdown_converter(self):
        """Test error handling in pipeline context."""
        try:
            from agentic_rag import PipelineFactory

            factory = PipelineFactory()

            # Create pipeline with MarkItDown converter
            pipeline_spec = [{"type": "CONVERTER.MARKITDOWN_PDF"}]

            spec, pipeline = factory.create_pipeline_from_spec(
                pipeline_spec, "markitdown_error_test"
            )

            assert spec.name == "markitdown_error_test"
            assert len(spec.components) == 1
            assert spec.components[0].name == "markitdown_pdf_converter"

            print("✅ MarkItDown converter pipeline creation successful")

        except ImportError as e:
            pytest.skip(f"Dependencies not available: {e}")


if __name__ == "__main__":
    # Run tests manually for debugging
    test_instance = TestMarkItDownPDFToDocument()
    test_instance.setup_method()

    try:
        test_instance.test_markitdown_converter_initialization()
        test_instance.test_markitdown_initialization_success()
        test_instance.test_component_available_in_registry()
        test_instance.test_run_empty_sources()
        print("\n🎉 All manual MarkItDown tests completed!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        test_instance.teardown_method()
