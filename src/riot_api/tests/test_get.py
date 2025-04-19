import pytest
from riot_api import get


@pytest.mark.asyncio
async def test_get_match_timeline_webtest():
    players = await get.fetch_with_rate_limit('players', platform='euw1')
    puuid = players["entries"][0]["puuid"]

    match_ids = await get.fetch_with_rate_limit('player_match_ids', region='europe', puuid=puuid, count=1)
    match_id = match_ids[0]
    
    match_info = await get.fetch_with_rate_limit('match_info', region='europe', match_id=match_id)
    match_timeline = await get.fetch_with_rate_limit('match_timeline', region='europe', match_id=match_id)
    
    assert isinstance(match_info, dict)
    assert isinstance(match_timeline, dict)
