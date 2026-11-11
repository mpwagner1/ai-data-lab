import ingestion


def test_ingestion_package_imports() -> None:
    assert ingestion.__name__ == "ingestion"
