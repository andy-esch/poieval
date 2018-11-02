"""POI Evaluation utility functions"""

from .colors import bcolors

def nearest_other(source, target, context):
    """Calculates the distance of the nearest point in a target datasest
    as compared to a source dataset. It outputs the straight line geometry
    and straight line distance between the two.

    Args:
        source (str): SQL query of an origin data source
        target (str): SQL auery of a target data source

    returns:
        pandas.DataFrame
    """
    nearest_q = f'''
        SELECT
          ST_MakeLine(source.the_geom, target.the_geom) as the_geom,
          ST_MakeLine(
            source.the_geom_webmercator,
            target.the_geom_webmercator
          ) as the_geom_webmercator,
          source.cartodb_id as cartodb_id,
          target.cartodb_id as target_cartodb_id,
          ST_Distance(
            geography(source.the_geom),
            geography(target.the_geom)
          ) as distance,
          9 * (
            ST_Distance(
              geography(source.the_geom),
              geography(target.the_geom)
            ) - 0.277
          ) / (908 - 0.277) + 1 as marker_size
        FROM (
          SELECT cartodb_id, the_geom, the_geom_webmercator
          FROM ({source}) as _w
        ) as source
        CROSS JOIN LATERAL (
          SELECT cartodb_id, the_geom, the_geom_webmercator
          FROM ({target}) as _w
          ORDER BY source.the_geom_webmercator <-> the_geom_webmercator
          LIMIT 1
        ) as target
    '''
    return context.query(nearest_q, decode_geom=True)

def eval_nearest(nearest_df):
    """"""
    perc_above = 100.0 * sum(nearest_df.distance > 150) / nearest_df.shape[0]
    perc_below = 100.0 * sum(nearest_df.distance <= 150) / nearest_df.shape[0]
    perc_close = 100.0 * sum(nearest_df.distance <= 25) / nearest_df.shape[0]
    num_misses = sum(nearest_df.distance > 25)
    summary = (
        f'> 150 meters: {perc_above:.2f}%\n'
        f'<= 150 meters: {perc_below:.2f}%\n'
        f'<= 25 meters: {perc_close:.2f}%\n'
        f'num misses: {bcolors.BOLD}{num_misses}{bcolors.ENDC} of '
        f'{nearest_df.shape[0]}'
    )
    return summary


def special_pois_summary(
        provider: str, region='nyc': str,
        selected_pois='poi_test_nyc_locations': str,
        ):
    SELECTED_POIS = selected_pois
    spois = cc.read(SELECTED_POIS)
    PROVIDER = provider
    provider_source = data[PROVIDER][region]
    address_col = data[PROVIDER]['address']
    store_name = data[PROVIDER]['name']

    q = f'''
        SELECT
            *,
            ST_Distance(
              the_geom::geography,
              CDB_LatLng({{lat}}, {{lng}})::geography
            ) as distance
        FROM ({provider_source}) as _w
        ORDER BY the_geom <-> CDB_LatLng({{lat}}, {{lng}})
    '''

    for row in spois.iterrows():
        q_formatted = q.format(
            name=row[1].loc['name'],
            lat=row[1].latitude,
            lng=row[1].longitude
        )
        ans = cc.query(q_formatted)
        if len(ans) > 0:
            if ans.iloc[0].loc['distance'] > 1000:
                print(
                    f"* {row[1].loc['name']} exists but its "
                    f"{ans.iloc[0].loc['distance']} meters away: "
                    f"{ans.iloc[0].loc['street_address']} vs "
                    f"{row[1].address}")
            else:
                print(f"* {row[1].loc['name']} matches: {ans.iloc[0].loc['street_address']} vs {row[1].formatted_address}")
        else:
            print(f"* No matches for {row[1].loc['name']}")
