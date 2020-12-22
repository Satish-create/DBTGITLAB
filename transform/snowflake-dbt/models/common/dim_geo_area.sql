{{ generate_single_field_dimension(model_name="prep_sfdc_account",
                                   id_column="dim_geo_area_name_source",
                                   id_column_name="dim_geo_area_id",
                                   dimension_column="dim_geo_area_name_source",
                                   dimension_column_name="geo_area_name",
                                   where_clause=None)
}}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@msendal",
    updated_by="@paul_armstrong",
    created_date="2020-11-04",
    updated_date="2020-12-10"
) }}
