import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_ai_benchmark_run_smoke():
    """
    Integration smoke test to verify that the POST /api/v1/benchmarks/run endpoint 
    completes successfully when USE_AI_BENCHMARK_SELECTION=True (default), and the API
    returns the newly added selection_method and cluster_metadata fields.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as test_client:
        # Trigger a run
        response = await test_client.post("/api/v1/benchmarks/run")
        assert response.status_code == 201
        
        data = response.json()
        assert "benchmarks" in data
        
        # Verify AI Metadata properties are correctly surfaced
        benchmarks = data["benchmarks"]
        for btype, bm in benchmarks.items():
            if bm is not None:
                assert bm["selection_method"] == "ai_kmeans"
                assert isinstance(bm["cluster_metadata"], dict)
                assert "cluster_id" in bm["cluster_metadata"]
                assert "centroid_distance" in bm["cluster_metadata"]
                assert bm["land_transaction_id"] is not None
                assert bm["score_breakdown"] is not None
