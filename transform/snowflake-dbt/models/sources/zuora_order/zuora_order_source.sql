WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'order') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                       AS order_id,

      --Info
      accountid::VARCHAR AS account_id,
      billtocontactid::VARCHAR AS bill_to_contact_id,
      createdbyid::VARCHAR AS created_by_id,
      createdbymigration::VARCHAR AS created_by_migration,
      createddate::VARCHAR AS created_date,
      defaultpaymentmethodid::VARCHAR AS default_payment_method_id,
      description::VARCHAR,
      orderdate::VARCHAR AS order_date,
      ordernumber::VARCHAR AS order_number,
      parentaccountid::VARCHAR AS parent_account_id,
      soldtocontactid::VARCHAR AS sold_to_contact_id,
      state::VARCHAR,
      status::VARCHAR,
      updatedbyid::VARCHAR AS updated_by_id,
      updateddate::VARCHAR AS updated_date

    FROM source

)

SELECT *
FROM renamed