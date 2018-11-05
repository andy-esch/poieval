"""POI Evaluation utility functions"""

from .colors import bcolors

import json
import os
# import logging
from collections import Counter

# logging.basicConfig(level=logging.DEBUG)


class POIEval:
    def __init__(self, context):
        self.context = context
        dir_path = os.getcwd()
        with open(f'{dir_path}/poi-sources.json', 'r') as f:
            self.data = json.load(f)

    def nearest_other(self, source: str, target: str):
        """Calculates the distance of the nearest point in a target datasest
        as compared to a source dataset. It outputs the straight line geometry
        and straight line distance between the two. This is useful for
        comparing one POI dataset's collection of one chain to another's.

        Args:
            source (str): SQL query of an origin data source
            target (str): SQL query of a target data source

        returns:
            pandas.DataFrame
        """
        nearest_q = f'''
            SELECT
                the_geom,
                ST_Transform(the_geom, 3857) as the_geom_webmercator,
                distance,
                9 * (
                  distance - min(distance) over ()
                ) / (
                  max(distance) OVER () - min(distance) OVER ()
                ) + 1 as marker_size,
                source_cartodb_id as cartodb_id,
                source_cartodb_id,
                target_cartodb_id
            FROM (
                SELECT
                  ST_MakeLine(source.the_geom, target.the_geom) as the_geom,
                  source.cartodb_id as source_cartodb_id,
                  target.cartodb_id as target_cartodb_id,
                  ST_Distance(
                    geography(source.the_geom),
                    geography(target.the_geom)
                  ) as distance
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
            ) as _w
        '''
        return self.context.query(nearest_q, decode_geom=True)

    def eval_nearest(self, nearest_df):
        """"""
        df_size = nearest_df.shape[0]
        perc_above = 100.0 * sum(nearest_df.distance > 150) / df_size
        perc_below = 100.0 * sum(nearest_df.distance <= 150) / df_size
        perc_close = 100.0 * sum(nearest_df.distance <= 25) / df_size
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
            self,
            provider: str, region: str = 'nyc',
            selected_pois: str = 'poi_test_nyc_locations',
            ):
        spois = self.context.read(selected_pois)
        provider_source = self.data[provider]['region'][region]
        address_col = self.data[provider]['address']
        location_name = self.data[provider]['name']

        q = f'''
            SELECT
                {address_col} as street_address,
                ST_Distance(
                  the_geom::geography,
                  CDB_LatLng({{lat}}, {{lng}})::geography
                ) as distance,
                '{{poi_name}}' as name
            FROM ({provider_source}) as _w
            WHERE {location_name} ilike '%{{poi_name}}%'
            ORDER BY the_geom <-> CDB_LatLng({{lat}}, {{lng}})
        '''

        cntr = Counter({'hit': 0, 'nohit': 0, 'toofar': 0})
        for row in spois.iterrows():
            q_formatted = q.format(
                poi_name=row[1].loc['name'],
                lat=row[1].latitude,
                lng=row[1].longitude
            )
            ans = self.context.query(q_formatted)
            if len(ans) > 0:
                if ans.iloc[0].loc['distance'] > 500:
                    print(
                        f"* {row[1].loc['name']} exists but its "
                        f"{ans.iloc[0].loc['distance']:.0f} meters away: "
                        f"{ans.iloc[0].loc['street_address']} vs "
                        f"{row[1].formatted_address}"
                    )
                    cntr['toofar'] += 1
                else:
                    print(
                        f"* {row[1].loc['name']} matches "
                        f"({ans.iloc[0].loc['distance']:.0f} meters): "
                        f"{ans.iloc[0].loc['street_address']} vs "
                        f"{row[1].formatted_address}"
                    )
                    cntr['hit'] += 1
            else:
                print(f"* No matches for {row[1].loc['name']}")
                cntr['nohit'] += 1

        print(
            f"Summary: {cntr['hit']} hits, {cntr['toofar']} too far, and "
            f"{cntr['nohit']} misses, of a total of "
            f"{sum(cntr[t] for t in cntr)} locations."
        )

    def category_summary(self, provider: str, region: str):
        """"""
        source = self.data[provider]['region'][region]
        cat_col = self.data[provider]['category']
        q = f'''
            SELECT nullif({cat_col}, '') as category, count(*) as cnt
            FROM ({source}) as _w
            GROUP BY 1
            ORDER BY 2 DESC
        '''
        print(q)
        cat_summary = self.context.query(q)
        num_nulls = cat_summary[cat_summary['category'].isnull()].values[0]
        print(
            f'* {provider}\n'
            f'  * Number of null-valued category entries: {num_nulls}\n'
            f'  * Number of categories: {cat_summary.shape[0]}\n'
            f'  * Number of rare entries (fewer than five per): '
            f'{cat_summary[cat_summary.cnt <= 5].shape[0]}\n'

        )
