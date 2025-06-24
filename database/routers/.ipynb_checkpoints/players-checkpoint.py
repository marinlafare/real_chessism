# ROUTERS
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from database.operations.players import insert_player, read_player
from typing import Dict, Any

router = APIRouter()

@router.get("/player/{player_name}")
async def api_read_player_name(player_name:str) -> JSONResponse:
    profile = await read_player(player_name)
    if profile:
        return JSONResponse(content=profile)
    else:
        raise HTTPException(status_code=404, detail=f"Player '{player_name}' not found")

@router.get("/player/{player_name}/{feature}")
async def api_read_player_feature(player_name:str, feature:str) -> JSONResponse:
    profile = await read_player(player_name)
    if profile:
        if feature in profile:
            return JSONResponse(content=profile[feature])
        else:
            raise HTTPException(status_code=404, detail=f"Feature '{feature}' not found for player '{player_name}'")
    else:
        raise HTTPException(status_code=404, detail=f"Player '{player_name}' not found")

@router.post("/player")
async def api_create_player_profile(data: Dict[str, Any]) -> JSONResponse:
    player_data = await insert_player(data)

    if player_data:
        return player_data
    else:
        raise HTTPException(status_code=400, detail="Something happened trying to create the player idk")
