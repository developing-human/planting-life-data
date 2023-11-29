# Clears calculated data from the cache.  Does NOT clear 
# raw fetched data, as that is time consuming to gather
# and typically changes are related to parsing logic.

# -f silences errors for non-existent files
rm -f cache/usda/common-names/*
rm -f cache/usda/symbols/*
rm -f cache/wildflower/moisture/*