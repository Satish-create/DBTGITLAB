{{ create_pi_source_table(
    source=source('gitlab_data_yaml', 'ops_section_pi')
    )
}}

SELECT *
FROM intermediate_stage
