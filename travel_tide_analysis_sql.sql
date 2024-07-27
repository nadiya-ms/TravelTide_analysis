-- Cohort definition. All sessions beginning from 2023-01-04
WITH sessions_2023 AS (
  SELECT *
  FROM sessions s
  WHERE s.session_start>='2023-01-04'
),

-- Consider only users who have more than 7 sessions
filtered_users AS (
  SELECT user_id, COUNT(*)  
  FROM sessions_2023 s
  GROUP BY user_id
  HAVING COUNT(*) > 7
),

-- Join tables users, flights and hotels with filtered users 
session_base AS
(SELECT s.user_id,
 s.session_id,  s.trip_id, s.session_start, s.session_end, 
 EXTRACT(EPOCH FROM s.session_end - s.session_start) AS session_duration, -- Period in seconds from session end and session start
 (s.session_end - s.session_start) AS session_duration_ts,  
 EXTRACT(DAY FROM s.session_end - u.sign_up_date) AS user_age_days, -- Days between session end and sign up date
 (s.session_end - u.sign_up_date) AS user_age,
 s.page_clicks,
 s.flight_discount, s.flight_discount_amount, s.hotel_discount, s.hotel_discount_amount, s.flight_booked, s.hotel_booked, s.cancellation,
 u.birthdate, u.gender, u.married, u.has_children, u.home_country, u.home_city, u.home_airport, u.home_airport_lat, u.home_airport_lon,u.sign_up_date,
 f.origin_airport, f.destination, f.destination_airport, f.seats, f.return_flight_booked, f.departure_time, f.return_time, f.checked_bags, f.trip_airline, 
 f.destination_airport_lat, f.destination_airport_lon,f.base_fare_usd,
 h.hotel_name, 
 CASE 
 WHEN h.nights < 0 and f.departure_time is not null and f.return_time is not null THEN extract('DAY' FROM age(f.return_time, f.departure_time)) -- Booked nights, calculated from departure_time and return_time period, if nights value is less than 0
 WHEN h.nights < 0 THEN 1 -- Booked 1 night, if nights value is less than 0 
 ELSE h.nights END AS nights, 
 
 h.rooms, h.check_in_time, h.check_out_time, h.hotel_per_room_usd AS hotel_price_per_room_night_usd

FROM sessions_2023 s
LEFT JOIN users u
ON s.user_id=u.user_id
LEFT JOIN flights f
ON s.trip_id=f.trip_id
LEFT JOIN hotels h
ON s.trip_id=h.trip_id
WHERE s.user_id IN (SELECT user_id FROM filtered_users)),

-- Cancelled trips
canceled_trips AS (
SELECT DISTINCT trip_id, user_id, session_id
FROM session_base
WHERE cancellation = TRUE 
),

-- Filter out not cancelled trips with the same trip_id as cancelled trips
not_canceled_trips AS(
SELECT *
FROM session_base
WHERE trip_id IS NOT NULL
AND trip_id NOT IN (SELECT trip_id FROM canceled_trips)
),

-- Group relevant data by user_id
user_base_session AS (
SELECT user_id,
SUM(user_age_days) AS total_user_age_days,  
SUM(page_clicks) AS num_clicks,
COUNT(DISTINCT session_id) AS num_sessions, -- unique session_id count
MAX(session_end) AS last_activity,
MAX(return_time) AS max_return_time,
MAX(check_out_time) AS max_checkout_time,
ROUND(AVG(page_clicks),2) AS avg_page_clicks,
ROUND(AVG(session_duration),2) AS avg_session_duration, -- Avg session duration in seconds
AVG(session_duration_ts) AS avg_session_duration_ts  
FROM session_base
GROUP BY user_id
),

-- Conversion rate, dividing the number of booked trips (not cancelled) by total number of browsing sessions
conversion_rate AS (
SELECT nct.user_id,
COUNT(DISTINCT nct.trip_id) total_booked_trips,
ROUND((COUNT(DISTINCT nct.trip_id)::numeric/COUNT(DISTINCT sb.session_id)::numeric), 2) AS conversion_rate
FROM not_canceled_trips AS nct
JOIN session_base AS sb
ON nct.user_id = sb.user_id
GROUP BY nct.user_id
),

-- Cancellation proportion, returns NULL for users who didn't book any trip 
cancellation_proportion AS 
(SELECT user_id, 
 ROUND(COUNT(DISTINCT CASE WHEN cancellation THEN trip_id END)::numeric /
    	      NULLIF(COUNT(DISTINCT CASE WHEN NOT cancellation THEN trip_id END), 0), 2) AS cancellation_proportion
FROM session_base
GROUP BY user_id
),

-- Relevant data for trips characteristics
user_base_trip AS
(SELECT user_id, 
COUNT(DISTINCT trip_id) AS num_trips, 
SUM(CASE WHEN flight_booked AND return_flight_booked THEN 2  -- 2, if outbound and return flights have been booked
     WHEN flight_booked THEN 1 ELSE 0 END) AS num_flights,   -- 1, if flight have been booked without return flight, otherwise 0 num flights
SUM(CASE WHEN hotel_booked THEN 1 ELSE 0 END) AS num_hotels, -- 1, if hotel have been booked, otherwise 0
 
COALESCE((SUM((hotel_price_per_room_night_usd * nights * rooms) * (1 - (CASE WHEN hotel_discount_amount IS NULL THEN 0 ELSE hotel_discount_amount END)))), 0) AS money_spend_hotel, -- hotel costs minus discount
COALESCE((SUM((base_fare_usd) * (1 - (CASE WHEN flight_discount_amount IS NULL THEN 0 ELSE flight_discount_amount END)))), 0) AS money_spend_flight, -- flight costs minus discount
 
COALESCE((SUM((hotel_price_per_room_night_usd * nights * rooms) * (1 - (CASE WHEN hotel_discount_amount IS NULL THEN 0 ELSE hotel_discount_amount END)))), 0)+
COALESCE((SUM((base_fare_usd) * (1 - (CASE WHEN flight_discount_amount IS NULL THEN 0 ELSE flight_discount_amount END)))), 0) AS total_money_spend, -- total costs (hotel + flights) minus discount

ROUND(COALESCE((AVG((hotel_price_per_room_night_usd * nights * rooms) * (1 - (CASE WHEN hotel_discount_amount IS NULL THEN 0 ELSE hotel_discount_amount END)))), 0), 2) AS avg_money_spend_hotel, -- average hotel costs
ROUND(COALESCE((AVG((base_fare_usd) * (1 - (CASE WHEN flight_discount_amount IS NULL THEN 0 ELSE flight_discount_amount END)))), 0), 2) AS avg_money_spend_flight, -- average flight costs

ROUND(AVG(flight_discount_amount),2) AS avg_flight_discount_amount,
ROUND(AVG(hotel_discount_amount),2) AS avg_hotel_discount_amount,

SUM(seats) AS num_flight_seats,
SUM(checked_bags) AS num_checked_bags,
SUM(rooms) AS num_hotel_rooms,

ROUND(AVG(seats),2) AS avg_flight_seats,
ROUND(AVG(checked_bags),2) AS avg_checked_bags,
ROUND(AVG(rooms),2) AS avg_hotel_rooms,
 
ROUND(AVG(EXTRACT(DAY FROM (CASE WHEN departure_time IS NULL THEN check_in_time ELSE departure_time END) - session_end)), 2) AS avg_duration_time_after_booking, -- duration time in days after booking until departure time or check in time
 
ROUND(COUNT(flight_discount_amount)::numeric/COUNT(flight_discount)::numeric, 2) AS discount_flight_proportion,
ROUND(COUNT(hotel_discount_amount)::numeric/COUNT(hotel_discount)::numeric, 2) AS discount_hotel_proportion, 

-- Calculate average trip duration in days
(CASE WHEN
ROUND(AVG(EXTRACT(DAY FROM (check_out_time - check_in_time))),2) < 0 THEN ROUND(AVG(EXTRACT(DAY FROM (check_out_time - check_in_time))),2) * -1 -- multiplicate days with -1, if check out time is older than check in time to get positive number due to incorrect data in DB
 ELSE ROUND(AVG(EXTRACT(DAY FROM (check_out_time - check_in_time))),2) END) AS avg_hotel_stay_duration,
ROUND(AVG(EXTRACT(DAY FROM (return_time - departure_time))),2) AS avg_trip_duration,

AVG(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)) AS avg_km_flown, -- using provided function haversine_distance to compute average flown distance
SUM(flight_discount_amount*base_fare_usd)/SUM(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)) AS ads_per_km, -- average dollar saved per kilometre

-- proportion of weekend trips, when the departure time is on Fridays or Saturdays, and return_time is on Sundays or Mondays and the duration of the trip is less or equal three days
ROUND(SUM( CASE WHEN EXTRACT (DOW FROM COALESCE(departure_time, check_in_time)) IN (5, 6) AND EXTRACT(DOW FROM COALESCE(return_time, check_out_time)) IN (0, 1) AND 
 EXTRACT(DAY FROM COALESCE(return_time, check_out_time) - COALESCE(departure_time, check_in_time)) <= 3 -- Find weekend trips
 THEN 1 ELSE 0 END)::numeric / COUNT(trip_id), 2) AS weekend_proportion                                 -- and calculate weekend proportion
 
FROM not_canceled_trips
GROUP BY user_id                                                         
),

-- Calculate bargain hunter index
user_bargain_hunter_index AS
(SELECT user_id, ads_per_km * discount_flight_proportion * avg_flight_discount_amount AS bargain_hunter_index
FROM user_base_trip
 ),

-- Scaling data using formula: y = (x - min) / (max - min)
scaled_data AS
(SELECT user_id, (money_spend_flight - (SELECT MIN(money_spend_flight) FROM user_base_trip) )
/ (SELECT max(money_spend_flight) - min(money_spend_flight) FROM user_base_trip) AS money_spend_flight_scale,
(money_spend_hotel - (SELECT min(money_spend_hotel) FROM user_base_trip) )
/ (SELECT max(money_spend_hotel)- min(money_spend_hotel) FROM user_base_trip) AS money_spend_hotel_scale,
(total_money_spend - (SELECT min(total_money_spend) FROM user_base_trip) )
/ (SELECT max(total_money_spend)- min(total_money_spend) FROM user_base_trip) AS money_spend_total_scale
FROM user_base_trip),

-- Calculating customer value
user_base_customer_value AS 
(SELECT user_id,
(money_spend_hotel+money_spend_flight)/num_trips AS customer_value
FROM user_base_trip
)

-- Select all calculated and scaled data together using left join over user_id and ignoring cancelled trips
SELECT 
ubs.user_id,
EXTRACT(YEAR FROM AGE(birthdate)) AS age,
gender,
married, 
has_children, 
home_country, 
home_city, 
home_airport,
sign_up_date,
total_user_age_days,
num_clicks,
num_sessions,
last_activity,
max_return_time,
max_checkout_time,
avg_page_clicks,
avg_session_duration, -- in seconds
avg_session_duration_ts,
conversion_rate, -- number of booked trips (not cancelled) / total number of browsing sessions
cancellation_proportion, 
num_trips,
num_flights,
num_hotels,
money_spend_hotel,
money_spend_flight,
total_money_spend,
avg_money_spend_hotel,
avg_money_spend_flight,
avg_flight_discount_amount,
avg_hotel_discount_amount,
num_flight_seats,
num_checked_bags,
num_hotel_rooms,
avg_flight_seats,
avg_checked_bags,
avg_hotel_rooms,
avg_duration_time_after_booking,
discount_flight_proportion,
discount_hotel_proportion,
avg_hotel_stay_duration,
avg_trip_duration,
avg_km_flown,
ads_per_km,
weekend_proportion,
bargain_hunter_index,
money_spend_flight_scale,
money_spend_hotel_scale,
money_spend_total_scale,
customer_value

FROM user_base_session AS ubs

LEFT JOIN conversion_rate AS cr ON ubs.user_id = cr.user_id
LEFT JOIN cancellation_proportion AS cp ON ubs.user_id = cp.user_id
LEFT JOIN user_base_trip AS ubt ON ubs.user_id = ubt.user_id
LEFT JOIN user_bargain_hunter_index AS ubhi ON ubs.user_id = ubhi.user_id
LEFT JOIN scaled_data sd ON ubs.user_id = sd.user_id
LEFT JOIN user_base_customer_value AS ubcv ON ubs.user_id = ubcv.user_id
LEFT JOIN users u ON ubs.user_id = u.user_id

WHERE ubs.user_id IN (SELECT user_id FROM not_canceled_trips)
;
