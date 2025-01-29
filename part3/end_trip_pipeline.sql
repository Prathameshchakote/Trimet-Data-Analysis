drop type if exists service_type;
drop type if exists tripdir_type;

create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
create type tripdir_type as enum ('Out', 'Back');
create table event_info (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type
);

