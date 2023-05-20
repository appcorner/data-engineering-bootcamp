with

orders_state as (

		select o.order_guid, a.state 

        from {{ ref('stg_greenery__orders') }} o
            , {{ ref('stg_greenery__addresses') }} a

        where o.address_guid = a.address_guid

)

, final as (

		select
            state
			, count(distinct order_guid) as orders_count

		from orders_state

        group by 1
        order by orders_count desc LIMIT 1

)

select * from final