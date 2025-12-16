def test_healthcheck_does_not_require_datastore():
    import backend.app as backend_app

    client = backend_app.app.test_client()
    response = client.get("/health")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
    assert backend_app._data_store_loaded is False

