with source as (

    select * from {{ source('ticket_tailor', 'events') }}

),

renamed as (

    select
        call_to_action,
        created_at,
        currency,
        description,
        -- end,
        id,
        images,
        name,
        object,
        online_event,
        payment_methods,
        private,
        -- start,
        status,
        tickets_available,
        ticket_groups,
        ticket_types,
        timezone,
        total_issued_tickets,
        total_orders,
        url,
        venue,
        _sdc_batched_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version

    from source

)

select * from renamed