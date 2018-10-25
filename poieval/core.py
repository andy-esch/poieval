"""POI Evaluation utility functions"""

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
    summary = '''
        > 150 meters: {perc_above:.2f}%
        <= 150 meters: {perc_below:.2f}%
        <= 25 meters: {perc_close:.2f}%
        num misses: {num_misses}
    '''
