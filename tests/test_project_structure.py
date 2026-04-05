from pathlib import Path


def test_data_files_exist():
    assert Path("data/customers.csv").exists()
    assert Path("data/products.csv").exists()


def test_etl_files_exist():
    assert Path("etl/load_seed_data.py").exists()
    assert Path("etl/transform_fact_orders.py").exists()
    assert Path("etl/transform_aggregates.py").exists()


def test_dashboard_exists():
    assert Path("dashboard/app.py").exists()


def test_dag_exists():
    assert Path("dags/full_pipeline_dag.py").exists()