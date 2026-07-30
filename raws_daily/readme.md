# RAWS Saipan Daily Meteorology Assets

RAWS Saipan daily meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/assets/meteorology/raws/saipan/daily

Timestep: daily

Image name format: YYYYMMDD

### Bands

| Band     | Description                          | Units   |
|----------|--------------------------------------|---------|
| TMAX     | Daily maximum air temperature (@ 2m) | C       |
| TMIN     | Daily minimum air temperature (@ 2m) | C       |
| TAVG     | Daily mean air temperature (@ 2m)    | C       |
| RH       | Relative Humidity                    | %       |
| SPFH     | Specific humidity                    | kg kg-1 |
| EA       | Vapor pressure                       | Pa      |
| PRESSURE | Surface level pressure               | Pa      |
| WIND     | Wind speed (@ 10m)                   | m s-1   |
| RS       | Surface downward shortwave radiation | W m-2   |
| PRECIP   | Total precipitation                  | mm      |
| ETO      | ASCE reference ET (grass)            | mm      |
| ETR      | ASCE reference ET (alfalfa)          | mm      |

### Daily Aggregation

The daily aggregation starts (and ends) at 12 UTC to better represent the day for Saipan.  The start date is lagged by one day because of the 12 UTC start time. 

Reference ET was computed at the hourly timestep and then summed to the day.

## Availability

RAWS Saipan is currently available for 2024-01-25 to 2026-04-14 in GEE, but data for to 2020-01-01 to 2023-07-31 is available and could be ingested.

### References

