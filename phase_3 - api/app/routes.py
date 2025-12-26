# app/routes.py

from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import ORJSONResponse, FileResponse
import os
from app.models import SearchQuery, GraphSearchQuery
from app.store import store
from app.services.search import run_search, get_entity_details, run_graph_search

router = APIRouter()

@router.get("/health/ready", tags=["Health"])
def get_readiness_status():
    """
    Readiness probe to check if the initial data load is complete.
    """
    if store.is_ready:
        return {"status": "ready"}
    return ORJSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"status": "loading_data"}
    )

@router.get("/filters/options", tags=["Metadata"])
def get_filter_options():
    """
    Returns available filter options for the frontend.
    """
    if not store.is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data is still being loaded. Please try again later."
        )
    return store.cache.get("filter_options", {})

@router.get("/details/{entity}/{entity_id}", tags=["Details"])
def get_details(entity: str, entity_id: str):
    """
    Retrieves details for any given entity type and its ID.
    """
    if not store.is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data is still being loaded. Please try again later."
        )
    try:
        return get_entity_details(entity, entity_id)
    except KeyError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.post("/entities/search", tags=["Entities"])
def search_entities(query: SearchQuery):
    """
    Performs a filtered search for entities.
    """
    if not store.is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data is still being loaded. Please try again later."
        )
    try:
        return run_search(query)
    except RuntimeError as e:
         raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e)
        )

@router.post("/graphs/search", tags=["Graphs"])
def search_graph(query: GraphSearchQuery):
    """
    Generates and returns graph data (nodes and edges) based on filters.
    """
    if not store.is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data is still being loaded. Please try again later."
        )
    try:
        return run_graph_search(query)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while generating the graph: {str(e)}"
        )

# --- ROBUST ENDPOINT FOR SERVING IMAGES ---
@router.get("/images/{project_name}/{image_name}", tags=["Images"])
async def get_image(project_name: str, image_name: str):
    """
    Retrieves an image file from the local 'images' directory,
    automatically detecting the file extension.
    """
    # Construct the path to the project's image directory.
    project_dir = os.path.join("images", project_name)

    # Check if the project directory exists.
    if not os.path.isdir(project_dir):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project folder not found")

    # Search for a file that matches the image_name, ignoring the extension.
    for filename in os.listdir(project_dir):
        # os.path.splitext splits 'image.jpg' into ('image', '.jpg')
        file_root, file_ext = os.path.splitext(filename)
        if file_root == image_name:
            # Found a match! Construct the full path and return the file.
            full_path = os.path.join(project_dir, filename)
            return FileResponse(full_path)

    # If the loop completes without finding a file, raise the 404 error.
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Image not found")
