INSERT INTO dim_weather (weather_condition) VALUES
('Clear'),
('Rain'),
('Snow'),
('Fog'),
('Extreme');

INSERT INTO dim_incident (incident_type) VALUES
('No Incident'),
('Minor'),
('Major');

INSERT INTO dim_congestion (congestion_level) VALUES
('Low'),
('Medium'),
('High');


SELECT * FROM dim_weather;
SELECT * FROM dim_incident;
SELECT * FROM dim_congestion;