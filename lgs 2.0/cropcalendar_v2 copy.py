from sqlalchemy import create_engine
import datetime as dt
import pandas as pd
import numpy as np
import inspect
import math
import re


##############################################################
#                     DB ENGINES
##############################################################

eng_sql = create_engine('postgresql://postgres:greenage@192.168.100.17:5432/dataofmysql')
eng_gridedpak = create_engine('postgresql://postgres:greenage@192.168.100.17:5432/gridedpak')
eng_era5 = create_engine('postgresql://postgres:greenage@192.168.100.17:5432/era5test')
eng_testdb = create_engine('postgresql://postgres:greenage@192.168.100.17:5432/testdb')
eng_moin_weather = create_engine('postgresql://postgres:greenage@192.168.100.17:5432/moin_weather')
eng_moin_weather_local = create_engine('postgresql://postgres:greenage@localhost:5432/moin_weather')

##############################################################
#                     Functions
##############################################################


def debug_print(char, *args, fmt=None):
    """
    debug_print(char, [value], [line_len:int], [double_lines:bool], *, fmt:str|None)

    Behavior
    --------
    - Only `char`: prints `char * 100`.
    - `value` is a STRING LITERAL -> prints: "<char*10> text <char*10>"
    - `value` is an expression/variable -> prints:
        "<char*10> <expr> <char*10>"
        <formatted_value>
      (formatted via `fmt` if provided, e.g. fmt=".2f")

    Optional:
    - line_len (3rd positional): int → prints char*line_len before & after the main block
    - double_lines (4th positional): bool → prints those lines twice
    - fmt (keyword-only): passed to Python's format(value, fmt)
    """
    ch = "" if char is None else str(char)

    # No value -> just a 100-char line
    if not args:
        print(ch * 100)
        return

    value = args[0]

    # 3rd arg: line length
    line_len = None
    if len(args) >= 2:
        try:
            line_len = int(args[1])
        except (TypeError, ValueError):
            line_len = None

    # 4th arg: double lines
    double_lines = False
    if len(args) >= 3:
        double_lines = bool(args[2])

    # Try to recover expression text for value
    expr = "<?>"
    expr_is_literal = False
    try:
        frame = inspect.currentframe().f_back
        info = inspect.getframeinfo(frame)
        line = (info.code_context or [""])[0].strip()
        # Capture 2nd argument (value expression), allow up to two optional extras
        m = re.search(r'debug_print\s*\(\s*(.+?)\s*,\s*(.+?)(?:\s*,\s*.+?){0,2}\s*(?:,\s*fmt\s*=\s*.+?)?\)\s*$', line)
        if m:
            expr_text = m.group(2).split("#", 1)[0].strip()
            expr = expr_text
            # String literal detection
            expr_is_literal = bool(re.match(
                r'^[rRuUbBfF]*("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'|".*"|\'.*\')\s*$',
                expr_text,
                flags=re.DOTALL
            ))
    except Exception:
        pass

    # Apply fmt if provided and we're not in literal-message mode
    if fmt is not None and not expr_is_literal:
        try:
            value = format(value, fmt)
        except Exception:
            # If formatting fails, fall back to the raw value
            pass

    # Pre line(s)
    if line_len and ch:
        bar = ch * line_len
        print(bar)
        if double_lines:
            print(bar)

    banner = ch * 10 if ch else ""

    if expr_is_literal:
        # Message mode (single line)
        print(f"{banner} {value} {banner}" if banner else f"{value}")
    else:
        # Variable/expression mode: name line, then value on next line
        if banner:
            print(f"{banner} {expr} {banner}")
        else:
            print(expr)
        print(value)

    # Post line(s)
    if line_len and ch:
        bar = ch * line_len
        print(bar)
        if double_lines:
            print(bar)

# ======================================================================

def get_day_of_year(date):
    day = date.day
    month = date.month
    month_days_to_add = [0,31,28,31,30,31,30,31,31,30,31,30]
    if date.year % 4 == 0:
        month_days_to_add[2] = 29
    day_of_year  = 0
    for i in range(month):
        day_of_year += month_days_to_add[i]
    day_of_year += day
    return day_of_year

# ======================================================================

def get_grid_weather(sowing_date, crop_max_days, grid,day_of_year):
    # we can access the wehter on the basis of day number of the year so we take the sowdate and then 
    # we check if the sowdate year and sowdate + crop_max_days year are same than select the weather data to crop_max_days daynumber
    # if the sowdate year and sowdate + crop_max_days year are not same than select the weather data up to end of the year
    #   and then minus this from total required days and select the remaining days for the next year and select this from the start to the remaining days
    max_day_date = sowing_date + dt.timedelta(days=crop_max_days+5)
    if max_day_date.year != sowing_date.year:
        day_of_this_year =  dt.date(sowing_date.year,12,31) - sowing_date
        req_days_next_year = crop_max_days - int(day_of_this_year.days)
        query = f''' SELECT * FROM grid_weather_uptodate.weather_{grid}_ 
                                WHERE day_of_year >= '{day_of_year}' or day_of_year >'0' and day_of_year<='{req_days_next_year}' '''
        weather_df = pd.read_sql(query, eng_era5)
        final_weather_df1 = weather_df[weather_df['day_of_year']>=day_of_year]
        final_weather_df1.sort_values('day_of_year', inplace=True)

        final_weather_df2 = weather_df[(weather_df['day_of_year'] > 0) & (weather_df['day_of_year'] <= req_days_next_year)]
        final_weather_df2.sort_values('day_of_year', inplace=True)
        final_weather_df = pd.concat([final_weather_df1,final_weather_df2])
        final_weather_df.reset_index(drop=True,inplace=True)
        return final_weather_df
    
    else:
        query = f''' SELECT * FROM grid_weather_uptodate.weather_{grid}_ 
                                WHERE day_of_year >= '{day_of_year}' and day_of_year<='{day_of_year+crop_max_days}' '''
        weather_df = pd.read_sql(query, eng_era5)
        weather_df.sort_values('day_of_year', inplace=True)
        weather_df.reset_index(drop=True,inplace=True)
        return weather_df
    
# =================================================================================

def get_filtered_grid_weather(weather_df, sowing_date, crop_max_days, day_of_year):
    # The difference between this and get_grid_weather is that it takes the weather data franme and filter the weather
    # And the second difference is the dataframe that we get in this function is created from table that does not contaisn the column day_of_year
    crop_max_days = int(crop_max_days)
    year_days = []
    for date in weather_df['date']:
        year_days.append(get_day_of_year(date))
    weather_df['day_of_year'] = year_days


    max_day_date = sowing_date + dt.timedelta(days=crop_max_days+5)
    if max_day_date.year != sowing_date.year:
        day_of_this_year =  dt.date(sowing_date.year,12,31) - sowing_date
        req_days_next_year = crop_max_days - int(day_of_this_year.days)
        
        final_weather_df1 = weather_df[weather_df['day_of_year']>=day_of_year]
        final_weather_df1.sort_values('day_of_year', inplace=True)

        final_weather_df2 = weather_df[(weather_df['day_of_year'] > 0) & (weather_df['day_of_year'] <= req_days_next_year)]
        final_weather_df2.sort_values('day_of_year', inplace=True)
        final_weather_df = pd.concat([final_weather_df1,final_weather_df2])
        final_weather_df.reset_index(drop=True,inplace=True)
        return final_weather_df
    else:
        final_weather_df = weather_df[(weather_df['day_of_year'] >= day_of_year) & (weather_df['day_of_year'] <= day_of_year+crop_max_days)]
        final_weather_df.reset_index(drop=True,inplace=True)
        return final_weather_df

# =================================================================================
    
def get_merged_weather_from_acrued_forcast_predicted_moin_weather(acrued_weather_df, predicted_weather_df, forcasted_df,  sowing_date, day_of_year, crop_max_days):
    crop_max_days = int(crop_max_days) +4
    # sowDate = dt.datetime.strftime(sowing_date, '%Y-%m-%d')

    # ==============================================================
    # ❌ OLD CODE (Replaced due to duplicate index issue)
    # --------------------------------------------------------------
    # Problem:
    #   The previous implementation calculated `day_of_year`
    #   for all rows first, *before* filtering multi-year data.
    #   When accrued weather spanned across calendar years
    #   (e.g. 2024–2025), this produced duplicate `day_of_year`
    #   values (e.g., both 2024-02-29 and 2025-03-01 → 60).
    #   Pandas `.update()` cannot reindex with duplicate labels,
    #   which triggered:
    #       ValueError: cannot reindex on an axis with duplicate labels
    # 
    # if not acrued_weather_df.empty:
    #     acrued_days = []
    #     for date in acrued_weather_df['date']:
    #         acrued_days.append(get_day_of_year(dt.datetime.strptime(date, '%Y-%m-%d').date()))
    #     acrued_weather_df['day_of_year'] = acrued_days
    #     acrued_weather_df.drop(columns=['date'], inplace=True)
    #     acrued_weather_df.set_index('day_of_year', inplace=True)
    # ==============================================================
    
    # ✅ FIXED IMPLEMENTATION
    # --------------------------------------------------------------
    if not acrued_weather_df.empty:
        print("acrued_weather_df is not empty")

        # Ensure proper datetime dtype for 'date'
        acrued_weather_df['date'] = pd.to_datetime(acrued_weather_df['date'])

        # --- FIX IMPLEMENTED HERE ---
        # If accrued weather spans multiple years (e.g., 2024–2025),
        # keep only entries from the most recent year to avoid duplicate day_of_year values and ensure index uniqueness.
        if acrued_weather_df['date'].dt.year.nunique() > 1:
            print("⚠️ Accrued weather spans multiple years. Deduplicating by day_of_year instead of dropping entire year.")
            acrued_weather_df['day_of_year'] = acrued_weather_df['date'].dt.dayofyear
            # Keep only the last entry per day_of_year (most recent year wins)
            acrued_weather_df = (
                acrued_weather_df
                .sort_values('date')
                .drop_duplicates(subset='day_of_year', keep='last')
                .copy()
            )
        else:
            acrued_weather_df['day_of_year'] = acrued_weather_df['date'].dt.dayofyear

        # Now safely compute day_of_year *after* filtering
        acrued_weather_df['day_of_year'] = acrued_weather_df['date'].dt.dayofyear

        # Drop the date column (no longer needed after indexing)
        acrued_weather_df = acrued_weather_df.drop(columns=['date']).copy()

        # Set the day_of_year as the index for alignment
        acrued_weather_df.set_index('day_of_year', inplace=True)
    # ==============================================================
    
    if not forcasted_df.empty:
        forcasted_days = []
        for date in forcasted_df['date']:
            forcasted_days.append(get_day_of_year(dt.datetime.strptime(date, '%Y-%m-%d').date()))
        forcasted_df['day_of_year'] = forcasted_days
        forcasted_df.drop(columns=['date'], inplace=True)
        forcasted_df.set_index('day_of_year', inplace=True)
        

    predicted_weather_df
    predicted_weather_df.set_index('day_of_year', inplace=True)
    predicted_weather_df.update(acrued_weather_df)
    predicted_weather_df.update(forcasted_df)

    if day_of_year+crop_max_days > 365:
        final_weather_df1 = predicted_weather_df.loc[day_of_year:365]
    
        next_year_days = crop_max_days - (365-day_of_year)
        final_weather_df2 = predicted_weather_df.loc[0:next_year_days]
        final_weather_df = pd.concat([final_weather_df1, final_weather_df2])
        final_weather_df.reset_index(inplace=True)
    else:
        final_weather_df = predicted_weather_df.loc[day_of_year:day_of_year+crop_max_days]
    date = pd.date_range(start=sowing_date, periods=crop_max_days+1, freq='D').strftime('%Y-%m-%d')
    final_weather_df['date'] = date
    return final_weather_df

##############################################################

def get_merged_weather_from_acrued_forcast_predicted_moin_weather_v2(
    acrued_weather_df,
    predicted_weather_df,
    forecasted_df,
    sowing_date,
    sowing_day_of_year,
    crop_max_days
):
    # NORMALIZE INPUT PARAMETERS
    crop_max_days = int(crop_max_days) + 5  # small buffer
    sowing_date = pd.to_datetime(sowing_date)

    # CLEAN & INDEX ACCRUED WEATHER (PAST)
    if not acrued_weather_df.empty:
        acrued_weather_df["date"] = pd.to_datetime(acrued_weather_df["date"])

        # Deduplicate across multiple years
        if acrued_weather_df["date"].dt.year.nunique() > 1:
            acrued_weather_df["day_of_year"] = acrued_weather_df["date"].dt.dayofyear
            acrued_weather_df = (
                acrued_weather_df
                .sort_values("date")
                .drop_duplicates(subset="day_of_year", keep="last")
            )
        else:
            acrued_weather_df["day_of_year"] = acrued_weather_df["date"].dt.dayofyear

        acrued_weather_df = (acrued_weather_df.drop(columns=["date"]).set_index("day_of_year"))

    # INDEX PREDICTED CLIMATOLOGY (FULL YEAR)
    predicted_weather_df = predicted_weather_df.copy()
    predicted_weather_df = predicted_weather_df.set_index("day_of_year")

    # this is the base layer: predicted
    merged_df = predicted_weather_df.copy()

    # CLEAN & INDEX FORECASTED WEATHER (NEAR FUTURE)
    if not forecasted_df.empty:
        forecasted_df["date"] = pd.to_datetime(forecasted_df["date"])
        forecasted_df["day_of_year"] = forecasted_df["date"].dt.dayofyear
        forecasted_df = forecasted_df.drop(columns=["date"]).set_index("day_of_year")

    # OVERWRITE PRIORITY CHAIN { predicted → (overwritten by) accrued → (overwritten by) forecast }
    if not acrued_weather_df.empty:
        merged_df.update(acrued_weather_df)

    if not forecasted_df.empty:
        merged_df.update(forecasted_df)

    merged_df.index = merged_df.index.where(merged_df.index != 0, 365)
    merged_df = merged_df.sort_index()

    # SLICE CONTIGUOUS WINDOW (multi-year safe)
    start_idx = sowing_day_of_year
    end_idx = sowing_day_of_year + crop_max_days

    if end_idx > 365:
        # Wrap to next year (for predicted climatology)
        first_part = merged_df.loc[start_idx:365]
        second_part = merged_df.loc[0:(end_idx - 365)]
        final_weather_df = pd.concat([first_part, second_part], axis=0)
    else:
        final_weather_df = merged_df.loc[start_idx:end_idx]

    # APPLY ACTUAL SIMULATION DATES
    final_dates = pd.date_range(
        start=sowing_date,
        periods=len(final_weather_df),
        freq="D"
    ).strftime("%Y-%m-%d")

    final_weather_df = final_weather_df.reset_index(drop=True)
    final_weather_df["date"] = final_dates

    return final_weather_df

##############################################################
#                     defined inputs
##############################################################

soiltextures = {
    'Clay': { 
        'FieldCapacity': 36,
        'WiltingPoint': 22,
    },
    'Silty Clay': {
        'FieldCapacity': 36,
        'WiltingPoint': 23,
    },
    'Sandy Clay': {
        'FieldCapacity': 32,
        'WiltingPoint': 18,
    },
    'Clay Loam':{
        'FieldCapacity': 38,
        'WiltingPoint': 24,
    },
    'Silty Clay Loam': {
        'FieldCapacity': 34,
        'WiltingPoint': 21,
    },
    'Sandy Clay Loam': {
        'FieldCapacity': 34,
        'WiltingPoint': 21,
    },
    'Loam': {
        'FieldCapacity': 25,
        'WiltingPoint': 12,
    },
    'Silt Loam': {
        'FieldCapacity': 29,
        'WiltingPoint': 15,
    },
    'Sandy Loam': {
        'FieldCapacity': 23,
        'WiltingPoint': 11,
    },
    'Silt': {
        'FieldCapacity': 32,
        'WiltingPoint': 18,
    },
    'Loamy Sand': {
        'FieldCapacity': 15,
        'WiltingPoint': 7,
    },
    'Sand': {
        'FieldCapacity': 12,
        'WiltingPoint': 5,
    }
    
}
##############################################################
#                     Daily Thermal Temperature
##############################################################
def getDTT(minT, maxT, crop_max_T, crop_base_T):
  AminT = min(crop_max_T, maxT)
  AmaxT = min(crop_max_T, minT)
  averageT = (AminT + AmaxT) / 2
  return averageT - crop_base_T

##############################################################
#                     Growing Degree Days
##############################################################
def getGDD(start,minT, maxT, crop_max_T, crop_base_T):
  days = []
  tempstore = 0
  days.append(getDTT(minT[start], maxT[start], crop_base_T, crop_max_T))
  for i in range(start+1, len(minT)):
    acc = getDTT(minT[i], maxT[i], crop_base_T, crop_max_T)
    days.append(acc + tempstore)
    tempstore = acc + tempstore
  return days
##############################################################
#                    Funtions Mathmatics
##############################################################

def DateToDayOfYear(month, day,year):
    months = [31,28,31,30,31,30,31,31,30,31,30,31]
    # if leap year 
    if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
        months[1] = 29
    days = 0
    for i in range(month-1):
        days += months[i]
    days += day
    return days
def e_s(T):
    # T is mean temperature in C
    return 0.6108 * math.exp((17.27 * T) / (T + 237.3))

def deltaAirPressure(T):
    # T is mean temperature in C
    return 4098 * e_s(T) / ((T + 237.3)**2)

def e_a(T, RH):
    # T is mean temperature in C
    # RH is relative humidity in %
    return (RH / 100) * e_s(T)

def azimuthAngle(day):
    # day is day of year
    return 0.409 * math.sin((2 * math.pi / 365) * day - 1.39)
    
def monthlessthan6coef(month,day):
    if month == 1:
        return day
    elif month == 2:
        return 31 + day
    elif month == 3:
        return 59 + day
    elif month == 4:
        return 90 + day
    elif month == 5:
        return 120 + day
    elif month == 6:
        return 151 + day
    else :
        return -1

def monthlygreaterthan7coef(month,day):
    if month == 7:
        return 181 + day
    elif month == 8:
        return 212 + day
    elif month == 9:
        return 243 + day
    elif month == 10:
        return 273 + day
    elif month == 11:
        return 304 + day
    elif month == 12:
        return 334 + day
    else:
        return -1

def dr_radian(monthlessthan6,monthgreaterthan7):
    if monthlessthan6 == -1:
        return 1 + 0.033 * math.cos(2 * math.pi / 365 * monthgreaterthan7)
    else:
        return 1 + 0.033 * math.cos(2 * math.pi / 365 * monthlessthan6)

def sigmaRadian(monthlessthan6,monthgreaterthan7):
    if monthlessthan6 == -1:
        return 0.409 * math.sin(2 * math.pi / 365 * monthgreaterthan7 - 1.39)
    else:
        return 0.409 * math.sin(2 * math.pi / 365 * monthlessthan6 - 1.39)

def W_S(sigmaRadian):
    return math.acos(-math.tan(0.445) * math.tan(sigmaRadian))

def R_S(drRadian,sigmaRadian,W_S):
    return 24 * 60 / math.pi * 0.082 * drRadian * (W_S * math.sin(0.445) * math.sin(sigmaRadian) + math.cos(0.445) * math.cos(sigmaRadian) * math.sin(W_S))

def N_Hour(ws):
    return 24 / math.pi * ws

def ETc(deltaAirPressure,extraterrestrialradiation,airPressurePerDegree,TempMean,windspeed2m,e_s,e_a,kc):
    return (((((0.408) * (deltaAirPressure)) * (extraterrestrialradiation - 0)) + ((((airPressurePerDegree) * (900 / (TempMean + 273))) * windspeed2m) * (e_s - e_a))) / (deltaAirPressure + (airPressurePerDegree * (1 + 0.34 * windspeed2m)))) * kc

###############################
###############################
###### Calculated Inputs ######
###############################
###############################


lat_radian = 0.0
atmosphericPressure = 0.0
atmosphericPressurePerDegree = 0.0


MAD = 0.6 # Management Allowed Deficit
TAW = {} # Total Available Water
RAW = {} # Root Available Water
TMC = {} # Threshold Moisture Content

for soiltexture in soiltextures:
    TAW[soiltexture] = (soiltextures[soiltexture]['FieldCapacity'] - soiltextures[soiltexture]['WiltingPoint'])
    RAW[soiltexture] = (TAW[soiltexture] * MAD)
    # fixed upto 1
    TMC[soiltexture] = soiltextures[soiltexture]['FieldCapacity'] - TAW[soiltexture] * MAD


##############################################################
##############################################################
#                   New Updated Crop Calender
##############################################################
##############################################################

# Extracts leading numeric value from strings like: '71: watery ripe', '65 (flowering)', '89 maturity', '45' 
def extract_numeric(value, default=0.0):
    if value is None:
        return default

    text = str(value).strip()
    match = re.match(r"^\d+(\.\d+)?", text)
    if match:
        return float(match.group(0))
    return default

##############################################################

def ExcelNutrientEngine(Daily_req_list, fert_list, soil_init, def_threshold, label):
        initPool = max(soil_init, def_threshold)
        running_pool = 0                        
        running_cum_req = 0
        running_cum_uptake = 0

        Daily_uptake = []
        Pool = []
        Cum_req = []
        Cum_uptake = []
        Efficiency = []

        for i in range(len(Daily_req_list)):

            req = Daily_req_list[i]
            fert_today = fert_list[i]

            prevPool = running_pool

            if prevPool == 0:
                # Reset to initial pool
                running_pool = initPool

                uptake = min(req, initPool)
            else:
                # Compute uptake based on previous pool BEFORE subtraction
                uptake = min(req, prevPool)

                running_pool = prevPool - uptake + fert_today
                if running_pool < 0:
                    running_pool = 0

            # store daily update
            Daily_uptake.append(round(uptake, 4))
            Pool.append(round(running_pool, 4))

            running_cum_req += req
            running_cum_uptake += uptake

            Cum_req.append(round(running_cum_req, 4))
            Cum_uptake.append(round(running_cum_uptake, 4))

            eff = running_cum_uptake / running_cum_req if running_cum_req > 0 else 0
            Efficiency.append(round(min(eff, 1), 4))

        return {
            "Daily_req": Daily_req_list,
            "Daily_uptake": Daily_uptake,
            "Pool": Pool,
            "Cum_req": Cum_req,
            "Cum_uptake": Cum_uptake,
            "Efficiency": Efficiency,
        }

##############################################################
#                     Crop Calendar
##############################################################

TODAY = dt.date.today()

def cropcalendarr(sowd, grid, crop, variety, grid_region, grid_soil, farm_id, elev, lat, lon, userUid):
    ##############################################################
    #                     Inputs required
    ##############################################################
    sow_date = dt.datetime.strptime(str(sowd) , '%Y-%m-%d').date()

    # If crop was sown more than ~180 days (6 months) ago, ignore it
    days_old = (TODAY - sow_date).days

    if days_old > 180:
        debug_print("=")
        debug_print(
            "=",
            f"⏸️  Skipping farm_id={farm_id}: sowing date {sow_date} is {days_old} days old (beyond active crop season).",
        )
        debug_print("=")
        # Minimal simulator output (safe structure)
        return {
            "farmUid": farm_id,
            "gridId": grid,
            "userUid": userUid,
            "is_crop_active": False,
            "crop_age": days_old,
            "message": "Crop age exceeded 180 days — inactive crop."
        }

    crop_active = True
    day_of_year = get_day_of_year(sow_date)
    grid = str(grid)
    crop = str(crop)
    variety = str(variety)
    grid_region = str(grid_region)
    soil_type = str(grid_soil).strip().lower()

    ##############################################################
    #                     Data From Database
    ##############################################################
 
    df = pd.read_sql(f'''
        SELECT
            c.crop_category,
            v.crop_fk, v.variety_eng, v.min_days, v.max_days, v.min_temp, v.base_temp, v.opt_temp, v.upper_temp, v.ripening_start_gdd, v.ripening_end_gdd, v.cwr_min, v.cwr_max, v.root_depth_cm, v.sowing_depth_cm, v.lai_start_gdd, v.lai_max_gdd, v.lai_end_gdd, v.lai_start_bbch, v.lai_max_bbch, v.lai_end_bbch, v.tillers_per_plant, v.leaf_area_per_shoot_m2, v.plant_to_plant_cm, v.row_spacing_cm, v.seed_per_spot, v.seed_weight_mg, v.germination_percentage, v.survival_ratio, v.n_deficiency_kg_ha, v.p_deficiency_kg_ha, v.k_deficiency_kg_ha, v.stress_tolerance_index, v.fruiting_start_bbch, v.harvest_index, v.dm_fm_ratio, v.sowing_start_season, v.sowing_end_season, v.yield_t_ha,
            s.main_stage, s.principal_stage, s.bbch_scale, s.crop_coefficient, s.k_ext_par, s.daily_n_kg_ha, s.daily_p_kg_ha, s.daily_k_kg_ha,
            ss.sub_stage, ss.start_gdd, ss.end_gdd
        FROM 
            lgs2.crop_varieties AS v
        JOIN lgs2.crops AS c
            ON v.crop_fk = c.crop_name
        JOIN
            lgs2.varieties_stages AS s ON v.variety_eng = s."variety_fk"
        JOIN
            lgs2.varieties_substages AS ss ON s.uuid = ss."stage_uuid"
        WHERE
            v.crop_fk = '{crop}'
            AND v.variety_eng = '{variety}'
        ORDER BY
            v.crop_fk,
            v.variety_eng,
            s.bbch_scale,
            s.main_stage,
            s.principal_stage,
            ss.sub_stage;
    ''', eng_moin_weather_local)

    if df.empty:
        debug_print("=")
        debug_print(
            "=",
            f"No crop variety / stages found for crop='{crop}', variety='{variety}'. Aborting cropcalendarr.",
        )
        debug_print("=")
        return

    ##############################################################
    #                     Crop Data
    ##############################################################
    
    # Single variety-level record
    v0 = df.iloc[0]

    crop_min_days = int(v0["min_days"])
    crop_max_days = int(v0["max_days"])

    crop_base_T   = float(v0["base_temp"])
    crop_opt_T   = float(v0["opt_temp"])
    crop_upper_T  = float(v0["upper_temp"])

    crop_root_depth_cm = float(v0["root_depth_cm"])
    crop_sowing_depth_cm = float(v0["sowing_depth_cm"])
    cwr_min = float(v0["cwr_min"])
    cwr_max = float(v0["cwr_max"])

    tillers_per_plant = float(v0["tillers_per_plant"])
    leaf_area_per_shoot_m2 = float(v0["leaf_area_per_shoot_m2"])
    plant_to_plant_cm   = float(v0["plant_to_plant_cm"])
    row_spacing_cm      = float(v0["row_spacing_cm"])
    seed_per_spot       = float(v0["seed_per_spot"])
    seed_weight_mg = float(v0["seed_weight_mg"])
    germination_pct = float(v0["germination_percentage"])
    survival_ratio = float(v0["survival_ratio"])

    n_def_kg_ha = float(v0["n_deficiency_kg_ha"])
    p_def_kg_ha = float(v0["p_deficiency_kg_ha"])
    k_def_kg_ha = float(v0["k_deficiency_kg_ha"])

    stress_tolerance_index = float(v0["stress_tolerance_index"])
    # fruiting_start_bbch = float(v0["fruiting_start_bbch"])
    fruiting_start_bbch = extract_numeric(v0["fruiting_start_bbch"], default=0.0)
    harvest_index = float(v0["harvest_index"])
    dm_fm_ratio = float(v0["dm_fm_ratio"])

    sowing_start_season = pd.to_datetime(v0["sowing_start_season"], errors="coerce")
    sowing_end_season = pd.to_datetime(v0["sowing_end_season"], errors="coerce")
    crop_ripening_start_gdd = float(v0["ripening_start_gdd"])
    crop_ripening_end_gdd   = float(v0["ripening_end_gdd"])

    yield_t_ha = float(v0["yield_t_ha"])
    crop_category = str(v0["crop_category"]).strip().lower()

    # Build substage lookup dictionary
    cropSubStages = {}
    for _, row in df.iterrows():
        sub = row["sub_stage"]

        cropSubStages[sub] = {
            "main_stage": row["main_stage"],
            "principal_stage": row["principal_stage"],
            "sub_stage": row["sub_stage"],
            "bbch_scale": row["bbch_scale"],
            "start_gdd": float(row["start_gdd"]),
            "end_gdd": float(row["end_gdd"]),
            "kc": float(row["crop_coefficient"]),
            "k_ext_par": float(row["k_ext_par"])
        }

    # NOW extract senescence substages from cropSubStages
    senescence_substages = [
        stg for stg in cropSubStages.values()
        if stg["principal_stage"].lower() == "senescence"
    ]
    senescence_substages = sorted(
        senescence_substages,
        key=lambda x: int(''.join(filter(str.isdigit, x["sub_stage"])))
    )
             
    ##############################################################
    #                    Weather Data
    ##############################################################
       
    # Past Weather (Acquired / Historical)
    acrued_weather_df = pd.read_sql(f"""
        SELECT
            *
        FROM
            acrued_weather.grid_{grid}
        WHERE
            date::date >= '{sowing_start_season.date()}'
            AND date::date <= '{TODAY}'
        ORDER BY
            date;
    """, eng_moin_weather_local)

    # Predicted (Climatology / Normals for the year)
    predicted_weather_df = pd.read_sql(f"""
        SELECT *
        FROM predicted_weather.grid_{grid}
        ORDER BY day_of_year;
    """, eng_moin_weather_local)

    # Forecasted Weather (Short-term)
    forecasted_weather_df = pd.read_sql(f"""
        SELECT 
            *
        FROM 
            forcast_weather.grid_id_{grid}
        ORDER BY 
            date;
    """, eng_moin_weather_local)

    # Merge all weather streams into a continuous future-aware sequence
    final = get_merged_weather_from_acrued_forcast_predicted_moin_weather_v2(
        acrued_weather_df,
        predicted_weather_df,
        forecasted_weather_df,
        sow_date,
        day_of_year,
        crop_max_days
    )

    if final is None or final.empty:
        debug_print("=")
        debug_print("=", "Merged weather dataframe is empty. Aborting cropcalendarr.")
        debug_print("=")
        return

    if "eto_fao" not in final.columns:
        debug_print("=")
        debug_print("=", "Merged weather dataframe has no 'eto_fao' column. Aborting cropcalendarr.")
        debug_print("=")
        return


    # Extract aligned arrays for simulation
    dates = final["date"].reset_index(drop=True)

    extraterrestrial_radiation_ra = final["extraterrestrial_radiation_ra"].tolist()
    min_temp  = final["temperature_2m_min"].astype(float).tolist()
    max_temp  = final["temperature_2m_max"].astype(float).tolist()
    avg_temp = [(mx + mn) / 2 for mx, mn in zip(max_temp, min_temp)]
    sunshine_duration = final["sunshine_duration"].tolist()
    daylight_duration = final["daylight_duration"].tolist()
    global_solar_radiation_rs = final["global_solar_radiation_rs"].tolist()
    relative_humidity_2m_max = final["relative_humidity_2m_max"].tolist()
    dew_point_2m_min = final["dew_point_2m_min"].tolist()
    vapour_pressure_deficit_max = final["vapour_pressure_deficit_max"].tolist()
    windspeed_10m_max = final["windspeed_10m_max"].tolist()
    soil_moisture_0_to_7cm_mean = final["soil_moisture_0_to_7cm_mean"].tolist()
    precipitation_sum = final["precipitation_sum"].tolist()
    rain_sum_predicted = final["rain_sum_predicted"].tolist()
    eto_fao = final["eto_fao"].tolist()

    # Adjust precipitation — ignore rainfall below threshold (e.g., < 5 mm)
    PRECIP_THRESHOLD = 5
    precipitation_adjusted = []
    for rain in precipitation_sum:
        if rain < PRECIP_THRESHOLD:
            precipitation_adjusted.append(0)
        else:
            precipitation_adjusted.append(rain * 0.8)

    ###############################################################################################################
    #                                           DTTOFSTAGES
    ###############################################################################################################

    # -------------------------------------------------------------
    # Build special weather stream ONLY for GDD-adjust calculation
    # -------------------------------------------------------------
    today_dt = pd.to_datetime(TODAY)
    sow_start = pd.to_datetime(sowing_start_season)
    sow_end   = pd.to_datetime(sowing_end_season)

    # Accrued weather: sow_start → today
    weather_past = acrued_weather_df.copy()
    weather_past["date"] = pd.to_datetime(weather_past["date"])
    weather_past = weather_past[
        (weather_past["date"] >= sow_start) & 
        (weather_past["date"] <= today_dt)
    ].copy()

    # Forecast: today+1 → sow_end
    forecasted_weather_df["date"] = pd.to_datetime(forecasted_weather_df["date"])
    weather_forecast = forecasted_weather_df[
        (forecasted_weather_df["date"] > today_dt) &
        (forecasted_weather_df["date"] <= sow_end)
    ].copy()

    # Last forecast date (or today if no forecast)
    if not weather_forecast.empty:
        last_fc_date = weather_forecast["date"].max()
    else:
        last_fc_date = today_dt

    # Predicted: from last forecast+1 → sow_end
    missing_dates = pd.date_range(
        start = last_fc_date + pd.Timedelta(days=1),
        end   = sow_end
    )

    predicted_weather_df["day_of_year"] = predicted_weather_df["day_of_year"].astype(int)
    pred_by_doy = predicted_weather_df.set_index("day_of_year")

    rows = []
    for d in missing_dates:
        doy = d.timetuple().tm_yday
        if doy in pred_by_doy.index:
            row = pred_by_doy.loc[doy].copy()
            row["date"] = d
            rows.append(row)

    weather_predicted = pd.DataFrame(rows).copy()

    # Combine into one continuous weather dataset
    gdd_weather_df = pd.concat(
        [weather_past, weather_forecast, weather_predicted],
        ignore_index=True
    ).sort_values("date").reset_index(drop=True)

    # Compute daily GDD for gdd_weather_df
    gdd_weather_df["daily_gdd"] = np.maximum(
        0,
        (
            (
                np.minimum(crop_opt_T, gdd_weather_df["temperature_2m_min"]) +
                np.minimum(crop_opt_T, gdd_weather_df["temperature_2m_max"])
            ) / 2
        ) - crop_base_T
    )
    gdd_weather_df["daily_gdd"] = gdd_weather_df["daily_gdd"].fillna(0)

    # total observed GDD EXACTLY like Excel
    mask_gdd = (
        (gdd_weather_df["date"] >= sow_start) &
        (gdd_weather_df["date"] <= sow_end)
    )
    total_gdd_obs = gdd_weather_df.loc[mask_gdd, "daily_gdd"].sum()

    # Compute daily GDD for each source individually
    def compute_daily_gdd(df):
        if df is None or df.empty:
            return df
        
        out = df.copy()
        
        out["daily_gdd"] = np.maximum(
            0,
            (
                (
                    np.minimum(crop_opt_T, out["temperature_2m_min"]) +
                    np.minimum(crop_opt_T, out["temperature_2m_max"])
                ) / 2
            ) - crop_base_T
        )
        out["daily_gdd"] = out["daily_gdd"].fillna(0)
        return out

    weather_past      = compute_daily_gdd(weather_past)
    weather_forecast  = compute_daily_gdd(weather_forecast)
    weather_predicted = compute_daily_gdd(weather_predicted)

    # -------------------------------------------------------------
    # GDD Calculation
    # -------------------------------------------------------------
    try:
        final["daily_gdd"] = np.maximum(
            0,
            (
                (
                    np.minimum(crop_opt_T, final["temperature_2m_min"]) +
                    np.minimum(crop_opt_T, final["temperature_2m_max"])
                ) / 2
            ) - crop_base_T
        )

        final["daily_gdd"] = final["daily_gdd"].fillna(0)

        # Compute avg_daily_gdd exactly
        avg_daily_gdd = min(
            crop_ripening_end_gdd / crop_min_days,
            max(
                crop_ripening_start_gdd / crop_max_days,
                crop_base_T,
                (crop_ripening_start_gdd / crop_max_days
                + crop_ripening_end_gdd / crop_min_days) / 2
            )
        )

        # Expected GDD for sowing window
        if (
            pd.isna(sowing_start_season)
            or pd.isna(sowing_end_season)
            or sowing_end_season < sowing_start_season
        ):
            total_gdd_exp = np.nan
        else:
            total_days = (sowing_end_season - sowing_start_season).days + 1
            total_gdd_exp = avg_daily_gdd * total_days

        # Determine GDD adjustment factor
        tolerance = 0.10    # ±10% allowed
        min_bound = 0.85    # –15% limit
        max_bound = 1.15    # +15% limit

        if pd.isna(total_gdd_exp) or total_gdd_exp <= 0 or total_gdd_obs <= 0:
            gdd_adjust = 1.0
        else:
            raw_adj = total_gdd_exp / total_gdd_obs
            need_adjust = raw_adj if abs(raw_adj - 1) > tolerance else 1.0
            gdd_adjust = min(max_bound, max(min_bound, need_adjust))

        # Per-day multiplier: adjust only low-GDD days
        threshold_factor = 0.9
        threshold = avg_daily_gdd * threshold_factor

        final["apply_adj"] = (gdd_adjust != 1.0) & (final["daily_gdd"] < threshold)
        final["final_mult"] = np.where(final["apply_adj"], gdd_adjust, 1.0)

        # Compute adjusted_gdd
        final["adjusted_gdd"] = final["daily_gdd"] * final["final_mult"]
        final["adjusted_gdd"] = final["adjusted_gdd"].fillna(0)

        # Compute cumulative_gdd using adjusted_gdd
        adjusted_gdd = final["adjusted_gdd"].to_numpy(dtype=float)
        cumulative_gdd = np.cumsum(adjusted_gdd)

        # -------------------------------------------------------------
        # EXTEND GDD & DATE SERIES UNTIL FULL SENESCENCE (90–99) ENDS
        # -------------------------------------------------------------
        max_required_gdd = max(s["end_gdd"] for s in senescence_substages)

        while cumulative_gdd[-1] < max_required_gdd:
            next_gdd = cumulative_gdd[-1] + avg_daily_gdd
            cumulative_gdd = np.append(cumulative_gdd, next_gdd)

            next_date = pd.to_datetime(dates.iloc[-1]) + pd.Timedelta(days=1)
            dates = pd.concat([dates, pd.Series([next_date])], ignore_index=True)

        # -------------------------------------------------------------
        # EXTEND WEATHER USING PREDICTED WEATHER (CLIMATOLOGY)
        # -------------------------------------------------------------
        extra_days = len(cumulative_gdd) - len(eto_fao)

        if extra_days > 0:
            predicted = predicted_weather_df.copy()
            predicted["day_of_year"] = predicted["day_of_year"].astype(int)
            pred_map = predicted.set_index("day_of_year")

            last_date = pd.to_datetime(dates.iloc[-1])

            for j in range(extra_days):
                next_date = last_date + pd.Timedelta(days=1)
                doy = next_date.timetuple().tm_yday

                # If DOY not in table, wrap around
                if doy not in pred_map.index:
                    doy = 1

                row = pred_map.loc[doy]

                # Extend every weather variable
                eto_fao.append(float(row["eto_fao"]))
                global_solar_radiation_rs.append(float(row["global_solar_radiation_rs"]))
                min_temp.append(float(row["temperature_2m_min"]))
                max_temp.append(float(row["temperature_2m_max"]))
                avg_temp.append(
                    (float(row["temperature_2m_min"])
                + float(row["temperature_2m_max"])) / 2
                )
                relative_humidity_2m_max.append(float(row["relative_humidity_2m_max"]))
                vapour_pressure_deficit_max.append(float(row["vapour_pressure_deficit_max"]))
                windspeed_10m_max.append(float(row["windspeed_10m_max"]))
                precipitation_adjusted.append(0)  # Excel assumes no rain unless forecast

                sunshine_duration.append(float(row["sunshine_duration"]))
                daylight_duration.append(float(row["daylight_duration"]))

                last_date = next_date
        
    except Exception as e:
        debug_print("=")
        debug_print("=", f"❌ ERROR computing adjusted GDD → {e}")
        debug_print("=")
        return

    DttofStages = []

    for day_index, GDD in enumerate(cumulative_gdd):

        if day_index >= len(dates):
            break
        date_str = str(dates.iloc[day_index])

        for substage_key, stg in cropSubStages.items():
            if stg["start_gdd"] <= GDD <= stg["end_gdd"]:

                DttofStages.append({
                    "main_stage": stg["main_stage"],
                    "principal_stage": stg["principal_stage"],
                    "sub_stage": substage_key,
                    "bbch_scale": stg["bbch_scale"],
                    "day": day_index + 1,
                    "GDD": GDD,
                    "date": date_str
                })
                break

    if not DttofStages:
        debug_print("=")
        debug_print("=", "No DttofStages computed from GDD. Aborting cropcalendarr.")
        debug_print("=")
        return

    # -------------------------------------------------------------
    # PATCH: Extend stages through full Senescence (substage 90–99)
    # -------------------------------------------------------------

    # Identify index where original DttofStages ends
    last_index = len(DttofStages) - 1
    current_day = last_index + 1  # next day after last natural stage

    # Load Senescence substages (from DB) and sort correctly
    senescence_substages = [
        stg for stg in cropSubStages.values()
        if stg["principal_stage"].lower() == "senescence"
    ]

    senescence_substages = sorted(
        senescence_substages,
        key=lambda x: int(''.join(filter(str.isdigit, x["sub_stage"])))
    )

    # Track existing days to avoid duplicates
    existing_days = { entry["day"] for entry in DttofStages }

    # Continue adding senescence stages until weather/GDD ends
    while current_day < len(cumulative_gdd):

        GDD_now = cumulative_gdd[current_day]

        # Determine which senescence substage this GDD falls into
        stg = None
        for s in senescence_substages:
            if s["start_gdd"] <= GDD_now <= s["end_gdd"]:
                stg = s
                break

        # If GDD passed last stage start_gdd but no substage applies
        if stg is None and GDD_now >= senescence_substages[-1]["start_gdd"]:

            if (current_day + 1) in existing_days:
                current_day += 1
                continue   # ← IMPORTANT: skip but don't break the whole patch!

            stg = senescence_substages[-1]  # force BBCH 99


        # If NO substage matches (GDD > end_gdd), use last senescence stage
        if stg is None:
            stg = senescence_substages[-1]


        # PREVENT DUPLICATE DAYS IN DttofStages
        day_number = current_day + 1

        if day_number in existing_days:
            current_day += 1
            continue

        # Append new senescence stage for this day
        DttofStages.append({
            "main_stage": stg["main_stage"],
            "principal_stage": stg["principal_stage"],
            "sub_stage": stg["sub_stage"],
            "bbch_scale": stg["bbch_scale"],
            "start_gdd": stg["start_gdd"],
            "end_gdd": stg["end_gdd"],
            "day": day_number,
            "GDD": GDD_now,
            "date": str(dates.iloc[current_day])
        })

        existing_days.add(day_number)
        current_day += 1

    # -------------------------------------------------------------
    # Root Depth Computation (GDD-driven)
    # -------------------------------------------------------------
    rootDepthList = []

    lai_start_gdd = df.loc[df.sub_stage.str.split(":").str[0].str.strip() == str(v0["lai_start_bbch"]).split(":")[0].strip(), "start_gdd"].iloc[0]
    lai_end_gdd = df.loc[df.sub_stage.str.split(":").str[0].str.strip() == str(v0["lai_end_bbch"]).split(":")[0].strip(), "end_gdd"].iloc[0]
    lai_max_start = df.loc[df.sub_stage.str.split(":").str[0].str.strip() == str(v0["lai_max_bbch"]).split(":")[0].strip(), "start_gdd"].iloc[0]
    lai_max_end   = df.loc[df.sub_stage.str.split(":").str[0].str.strip() == str(v0["lai_max_bbch"]).split(":")[0].strip(), "end_gdd"].iloc[0]
    lai_max_gdd = (lai_max_start + lai_max_end) / 2

    for cumGDD in cumulative_gdd:

        # Fraction completed (0 to 1)
        if lai_max_gdd > 0:
            frac = cumGDD / lai_max_gdd
            frac = max(0, min(1, frac))   # clamp 0–1
        else:
            frac = 0

        # Compute root depth for this day
        depth = crop_sowing_depth_cm + frac * (crop_root_depth_cm - crop_sowing_depth_cm)
        rootDepthList.append(depth)

    # -------------------------------------------------------------
    # Soil Water Balance (SWB) + Irrigation (mm)
    # -------------------------------------------------------------

    # Kc List (Stage-based crop coefficient lookup)
    crop_day = list(range(1, len(dates) + 1))

    # Irrigation is NOT allowed during the last X days before crop maturity
    IRRIGATION_STOP_BEFORE_MATURITY_DAYS = 8
    irrigation_cutoff_day = max(1, crop_max_days - IRRIGATION_STOP_BEFORE_MATURITY_DAYS)

    # kc list (crop coefficient per GDD)
    kc_list = []
    for GDD in cumulative_gdd:
        kc_value = 0
        for substage_key, stg in cropSubStages.items():
            if stg["start_gdd"] <= GDD <= stg["end_gdd"]:
                kc_value = float(stg["kc"])
                break
        kc_list.append(kc_value)

    # ETc list
    etc_list = []
    for i in range(len(dates)):
        if cumulative_gdd[i] == 0:
            etc_list.append(0)
            continue
        kc_today = kc_list[i]
        eto_today = float(eto_fao[i])
        etc_list.append(round(kc_today * eto_today, 2))
    
    # Load Soil Properties (usda_classes table)
    soil_df = pd.read_sql(f"""
        SELECT 
            field_capacity,
            wilting_point,
            threshold
        FROM soil.usda_classes
        WHERE texture = '{soil_type}'
        LIMIT 1;
    """, eng_moin_weather_local)

    if soil_df.empty:
        debug_print("=")
        debug_print("=", f"No soil properties found for soil_texture='{soil_type}'. Aborting cropcalendarr.")
        debug_print("=")
        return

    FC = float(soil_df["field_capacity"].iloc[0])        # %
    WP = float(soil_df["wilting_point"].iloc[0])         # %
    threshold = float(soil_df["threshold"].iloc[0])  # %

    # Load irrigation requirements
    soil_type_column = soil_type.lower().replace(" ", "_")
    soil_min_col = f'"{soil_type_column}_min"'
    soil_max_col = f'"{soil_type_column}_max"'

    irrig_req_df = pd.read_sql(f"""
        SELECT
            irrigation_min,
            {soil_min_col} AS cycle_min,
            {soil_max_col} AS cycle_max
        FROM crop.irrigation_requirements
        WHERE crop_name = '{crop}'
        LIMIT 1;
    """, eng_moin_weather_local)

    irrigation_min_mm = float(irrig_req_df["irrigation_min"].iloc[0])
    soil_min_cycles = float(irrig_req_df["cycle_min"].iloc[0])
    soil_max_cycles = float(irrig_req_df["cycle_max"].iloc[0])

    # initialize output lists
    SWB = []          # Soil Water Balance (mm)
    irrig = []        # Applied irrigation (mm)
    needed = []       # "Irrigate" / ""
    balance = []      # Deficit (mm needed)

    for i in range(len(dates)):
        root_depth_cm = rootDepthList[i]      # dynamic root depth (cm)
        eff_rain = precipitation_adjusted[i]  # effective rainfall (mm)
        etc_today = etc_list[i]               # crop evapotranspiration (mm)
        crop_day_i = crop_day[i]              # day number

        # Compute daily TAW and RAW (threshold)
        threshold_fraction = threshold / 100
        TAW_pct = FC - WP
        threshold_calc = threshold_fraction * TAW_pct
        threshold_mm = (threshold_calc / 100) * root_depth_cm * 10

        # First day initialization
        if i == 0:
            swb_today = threshold_mm
            SWB.append(swb_today)
            irrig.append(0)
            needed.append("")
            balance.append(0)
            continue

        # Daily SWB update
        swb_prev = SWB[-1]
        irrig_prev = irrig[-1]
        swb_today = swb_prev + eff_rain - etc_today + irrig_prev
        SWB.append(swb_today)

        # Determine if irrigation is needed
        if (swb_today < threshold_mm) and (threshold_mm > 0) and (crop_day_i <= irrigation_cutoff_day):
            needed_today = "irrigate"
        else:
            needed_today = ""

        needed.append(needed_today)

        # Calculate deficit (balance)
        if needed_today == "irrigate":
            balance_today = round(threshold_mm - swb_today, 2)
        else:
            balance_today = 0

        balance.append(balance_today)

        # APPLIED irrigation
        if needed_today == "irrigate" and crop_day_i <= irrigation_cutoff_day:

            prev_applied_sum = sum(irrig)

            if irrigation_min_mm > 1:
                numerator = irrigation_min_mm
                denominator = max(
                    soil_min_cycles,
                    min(
                        irrigation_min_mm / threshold_mm,
                        soil_max_cycles
                    )
                )
                applied_today = numerator / denominator

            else:
                remaining = irrigation_min_mm - prev_applied_sum
                numerator = remaining
                denominator = (
                    max(
                        soil_min_cycles,
                        min(
                            remaining / threshold_mm,
                            soil_max_cycles
                        )
                    )
                    +
                    (soil_max_cycles - soil_min_cycles) / 2
                )
                applied_today = numerator / denominator

        else:
            applied_today = 0

        irrig.append(applied_today)

    # -------------------------------------------------------------
    # WATER_EFFICIENCY
    # -------------------------------------------------------------

    water_efficiency = []
    last_eff = 1.0

    for i in range(len(DttofStages)):

        principal = DttofStages[i]["principal_stage"]
        etc_today = etc_list[i]

        if not principal:
            water_efficiency.append(last_eff)
            continue

        # ETc never collapses to 0 mid-season
        if etc_today == 0:
            water_efficiency.append(last_eff)
            continue

        bal = balance[i]
        applied = irrig[i]
        needed_mm = balance[i]
        total_supply = bal + applied

        if needed_mm <= 0:
            eff = 1
        elif total_supply >= 0.8 * needed_mm:
            eff = 1
        else:
            eff = total_supply / max(1, needed_mm)

        eff = max(0.0, min(1.0, eff))

        water_efficiency.append(eff)
        last_eff = eff

    # -------------------------------------------------------------
    # Compute LAI
    # -------------------------------------------------------------
        
    if plant_to_plant_cm > 0 and row_spacing_cm > 0:
        plants_per_m2 = 10000 / (row_spacing_cm * plant_to_plant_cm)
    else:
        plants_per_m2 = 0

    seed_rate_m2 = plants_per_m2 * seed_per_spot
    seed_weight_kg_ha = round((seed_weight_mg / 1_000_000) * seed_rate_m2 * 10_000, 2)
    effective_plants_m2 = (seed_rate_m2 * germination_pct) / 100
    shoots_per_m2 = effective_plants_m2 * tillers_per_plant
    shoot_bearing_m2 = shoots_per_m2 * survival_ratio

    # LAI Components (Start, Max, End)
    try:
        start_lai = max(0.05, min(0.5, leaf_area_per_shoot_m2 * shoot_bearing_m2))
    except:
        start_lai = 0.2

    max_lai = round(shoot_bearing_m2 * leaf_area_per_shoot_m2, 2)

    if max_lai >= 1.2:
        end_lai = round(0.85 * max_lai, 2)
    else:
        end_lai = 1.0

    # -------------------------------------------------------------
    # Daily LAI curve
    # -------------------------------------------------------------

    lai_list = []

    # Density term (seeds × germination × tillers_per_plant × leaf area)
    germ_frac = germination_pct / 100
    LAI_den = seed_rate_m2 * germ_frac * tillers_per_plant * leaf_area_per_shoot_m2

    for i, gdd in enumerate(cumulative_gdd):

        if i < len(DttofStages):
            if not DttofStages[i].get("sub_stage"):
                lai_list.append(None)
                continue

        # Phase 0 — Before LAI starts → LAI = 0
        if gdd < lai_start_gdd:
            lai_base = start_lai * (gdd / max(1, lai_start_gdd))

        # Piecewise LAI curve vs GDD

        # Growth: start_lai → max_lai
        elif gdd <= lai_max_gdd:
            lai_base = start_lai + (max_lai - start_lai) * ((gdd - lai_start_gdd) / max(1, (lai_max_gdd - lai_start_gdd)))

        # Decline: max_lai → end_lai
        elif gdd <= lai_end_gdd:
            lai_base = max_lai + (end_lai - max_lai) * ((gdd - lai_max_gdd) / max(1, (lai_end_gdd - lai_max_gdd)))

        # Past maturity → hold end LAI
        else:
            lai_base = end_lai

        # Density Factor
        if lai_base > 0:
            density_factor = min(1.0, LAI_den / lai_base)
        else:
            density_factor = 0.0

        lai_final = max(lai_base * density_factor, 0.0) # no negative lai
        lai_list.append(lai_final)

    # -------------------------------------------------------------
    # Compute daily fPAR
    # -------------------------------------------------------------
    
    fpar_list = []

    for i in range(len(dates)):
        
        # SAFELY get stage if it exists, otherwise reuse the last available principal_stage / sub_stage
        if i < len(DttofStages):
            stg = DttofStages[i]
        else:
            stg = DttofStages[-1]   # continue with last crop stage (senescence)

        # Get today's stage info
        principal = stg.get("principal_stage", "")
        substage = stg.get("sub_stage", "")

        # If principal stage missing → fPAR = 0
        if not principal:
            fpar_list.append(0.0)
            continue

        # REQUIRED SAFETY CHECK → substage must exist
        if substage not in cropSubStages:
            fpar_list.append(0.0)
            continue

        # Extract K_Ext_PAR for this sub-stage
        k_ext = cropSubStages[substage]["k_ext_par"]
        lai_today = lai_list[i] if lai_list[i] is not None else 0.0

        # fPAR equation
        if lai_today is None or lai_today <= 0:
            fpar_today = 0.0
        else:
            fpar_today = 1 - math.exp(-k_ext * lai_today)

        # cap at 0.85
        fpar_today = min(fpar_today, 0.85)

        # Prevent negative float round-off mistakes
        if fpar_today < 0:
            fpar_today = 0

        fpar_list.append(round(fpar_today, 4))

    # -------------------------------------------------------------
    # Temperature Stress (Cold / Heat / None)
    # -------------------------------------------------------------

    ts_type_list = []     # "Cold" / "Heat" / "None"
    ts_value_list = []    # numeric value (float)

    for Ta in avg_temp:

        Tb = crop_base_T
        Topt = crop_opt_T

        coldDeg = max(0, Tb - Ta)
        heatDeg = max(0, Ta - Topt)

        if coldDeg > 0:
            stressType = "Cold"
            stressValue = round(coldDeg, 1)
        elif heatDeg > 0:
            stressType = "Heat"
            stressValue = round(heatDeg, 1)
        else:
            stressType = "None"
            stressValue = 0.0

        ts_type_list.append(stressType)
        ts_value_list.append(stressValue)

    # -------------------------------------------------------------
    # NUTRIENT UPTAKE & FERTILIZER REQUIREMENTS
    # -------------------------------------------------------------

    BULK_DENSITY = 1.58               # g/cm³
    TOPSOIL_DEPTH_CM = 15             # depth
    SOIL_TO_KGHA = 0.1                # mg/kg → kg/ha

    # SOIL NPK (soil test or fallback)

    soil_test_df = pd.read_sql(f"""
        SELECT final_result, created_at
        FROM soil_sensors.farm_soil_tests
        WHERE farm_uid = '{farm_id}'
        ORDER BY created_at DESC
        LIMIT 1;
    """, eng_moin_weather_local)

    if not soil_test_df.empty:
        result_json = soil_test_df["final_result"].iloc[0]
        soil_n_mg_kg = float(result_json.get("n", 0))
        soil_p_mg_kg = float(result_json.get("p", 0))
        soil_k_mg_kg = float(result_json.get("k", 0))
        debug_print("=")
        debug_print("=", f"<< Using soil test NPK: N={soil_n_mg_kg}, P={soil_p_mg_kg}, K={soil_k_mg_kg} >>")
        debug_print("=")
    else:
        soil_opt_df = pd.read_sql(f"""
            SELECT nitrogen, phosphorus, potassium
            FROM soil.optimal_nutrients_15cm
            WHERE texture = '{soil_type}'
            LIMIT 1;
        """, eng_moin_weather_local).iloc[0]

        soil_n_mg_kg = float(soil_opt_df["nitrogen"])
        soil_p_mg_kg = float(soil_opt_df["phosphorus"])
        soil_k_mg_kg = float(soil_opt_df["potassium"])
        debug_print("=")
        debug_print("=", f"<< No soil test found; using optimal NPK >>")
        debug_print("=")

    # Convert soil nutrients → kg/ha
    soil_n_kg_ha = soil_n_mg_kg * BULK_DENSITY * TOPSOIL_DEPTH_CM * SOIL_TO_KGHA
    soil_p_kg_ha = soil_p_mg_kg * BULK_DENSITY * TOPSOIL_DEPTH_CM * SOIL_TO_KGHA
    soil_k_kg_ha = soil_k_mg_kg * BULK_DENSITY * TOPSOIL_DEPTH_CM * SOIL_TO_KGHA

    # -------------------------------------------------------------
    # DAILY REQUIREMENT TABLE FROM DB (Excel SUMIFS equivalent)
    # -------------------------------------------------------------

    Daily_N_req = []
    Daily_P_req = []
    Daily_K_req = []

    for stg in DttofStages:
        principal = stg["principal_stage"]
        mainstage = stg["main_stage"]

        if not principal:
            Daily_N_req.append(0)
            Daily_P_req.append(0)
            Daily_K_req.append(0)
            continue

        rows = df[
            (df["crop_fk"] == crop) &
            (df["variety_eng"] == variety) &
            (df["principal_stage"] == principal) &
            (df["main_stage"] == mainstage)
        ]

        # SUMIFS equivalent
        n_val = float(rows["daily_n_kg_ha"].sum()) if not rows.empty else 0
        p_val = float(rows["daily_p_kg_ha"].sum()) if not rows.empty else 0
        k_val = float(rows["daily_k_kg_ha"].sum()) if not rows.empty else 0

        Daily_N_req.append(n_val)
        Daily_P_req.append(p_val)
        Daily_K_req.append(k_val)

    # -------------------------------------------------------------
    # FERTIGATION BASED ON IRRIGATION EVENTS
    # -------------------------------------------------------------

    irrigation_days = sum(1 for x in needed if x.lower() == "irrigate")
    irrigation_days = max(1, irrigation_days)

    total_N_req = sum(Daily_N_req)
    total_P_req = sum(Daily_P_req)
    total_K_req = sum(Daily_K_req)

    N_applied = []
    P_applied = []
    K_applied = []

    for i in range(len(DttofStages)):
        if needed[i].lower() == "irrigate":
            N_applied.append(round(total_N_req / irrigation_days, 2))
            P_applied.append(round(total_P_req / irrigation_days, 2))
            K_applied.append(round(total_K_req / irrigation_days, 2))
        else:
            N_applied.append(0)
            P_applied.append(0)
            K_applied.append(0)

    # -------------------------------------------------------------
    # NUTRIENT POOL ENGINE (N, P, K)
    # -------------------------------------------------------------

    nutrient_results = {}

    nutrient_results["N"] = ExcelNutrientEngine(
        Daily_N_req, N_applied, soil_n_kg_ha, n_def_kg_ha, "N"
    )
    nutrient_results["P"] = ExcelNutrientEngine(
        Daily_P_req, P_applied, soil_p_kg_ha, p_def_kg_ha, "P"
    )
    nutrient_results["K"] = ExcelNutrientEngine(
        Daily_K_req, K_applied, soil_k_kg_ha, k_def_kg_ha, "K"
    )

    # Unpack
    Daily_N_req = nutrient_results["N"]["Daily_req"]
    Daily_P_req = nutrient_results["P"]["Daily_req"]
    Daily_K_req = nutrient_results["K"]["Daily_req"]

    N_efficiency = nutrient_results["N"]["Efficiency"]
    P_efficiency = nutrient_results["P"]["Efficiency"]
    K_efficiency = nutrient_results["K"]["Efficiency"]

    # -------------------------------------------------------------
    # NPK_EFFICIENCY  (minimum of N, P, K efficiencies)
    # -------------------------------------------------------------
    NPK_efficiency = []

    for i in range(len(DttofStages)):
        principal = DttofStages[i]["principal_stage"]

        if not principal:
            NPK_efficiency.append(0)
            continue

        n_eff = N_efficiency[i]
        p_eff = P_efficiency[i]
        k_eff = K_efficiency[i]

        npk_val = min(n_eff, p_eff, k_eff)
        NPK_efficiency.append(round(npk_val, 4))

    # -------------------------------------------------------------
    # PAR CALCULATION
    # -------------------------------------------------------------
        
    # Convert dates
    acrued_weather_df["date"] = pd.to_datetime(acrued_weather_df["date"], errors="coerce")

    mask = (
        (acrued_weather_df["date"] >= sowing_start_season) &
        (acrued_weather_df["date"] <= sowing_end_season)
    )

    # Average Rg based on DB sowing season range
    avg_Rg = acrued_weather_df.loc[mask, "global_solar_radiation_rs"].astype(float).mean()
    PAR_CONVERSION_FACTOR = 0.48
    PARs = avg_Rg * PAR_CONVERSION_FACTOR

    # -------------------------------------------------------------
    # RUE_MAX CALCULATION
    # -------------------------------------------------------------

    # Exponential attenuation coefficient for RUE scaling
    RUE_ALPHA = 0.6

    # Minimum fallback values for categories (if DB category missing)
    DEFAULT_K_DEF = 0.5           # Default canopy light extinction coefficient
    DEFAULT_RUE_MIN = 1.2         # Minimum allowed RUE
    DEFAULT_RUE_MAX_BASE = 1.8    # Base maximum RUE for unknown crop types

    # Canopy extinction coefficient per crop category
    k_def_map = {
        "cereal": 0.5,
        "legume vegetables": 0.7,
        "leaf vegetables": 0.7,
        "root vegetables": 0.6,
        "bulb vegetables": 0.6,
        "stem vegetables": 0.6,
        "fruit vegetables": 0.6,
        "fruit": 0.6,
        "oil seed": 0.5,
        "legume oil seed crop": 0.6,
        "fiber crop": 0.6,
    }

    # Lower bound for RUE per crop category
    RUE_min_map = {
        "cereal": 1.2,
        "legume vegetables": 1.2,
        "leaf vegetables": 1.5,
        "root vegetables": 1.8,
        "bulb vegetables": 1.5,
        "stem vegetables": 1.5,
        "fruit vegetables": 1.5,
        "fruit": 1.5,
        "oil seed": 1.2,
        "legume oil seed crop": 1.2,
        "fiber crop": 1.0,
    }

    # Maximum possible RUE before LAI correction
    RUE_max_base_map = {
        "cereal": 1.8,
        "legume vegetables": 2.5,
        "leaf vegetables": 3.0,
        "root vegetables": 3.5,
        "bulb vegetables": 3.0,
        "stem vegetables": 3.0,
        "fruit vegetables": 3.0,
        "fruit": 3.0,
        "oil seed": 2.5,
        "legume oil seed crop": 2.5,
        "fiber crop": 2.0,
    }

    # Assign category-specific parameters (fallback to defaults)
    k_def_value = k_def_map.get(crop_category, DEFAULT_K_DEF)                # Light extinction coefficient
    RUE_min_value = RUE_min_map.get(crop_category, DEFAULT_RUE_MIN)          # Minimum RUE constraint
    RUE_max_base_value = RUE_max_base_map.get(crop_category, DEFAULT_RUE_MAX_BASE)  # Upper RUE boundary

    # Mean of LAI at start, peak, and end
    LAI_mean = np.mean([start_lai, max_lai, end_lai])

    # Shoot-density-based LAI proxy
    if shoot_bearing_m2 > 0 and leaf_area_per_shoot_m2 > 0:
        LAI_proxy = shoot_bearing_m2 * leaf_area_per_shoot_m2
    else:
        # LAI_proxy = 0
        LAI_proxy = None

    # LAI used for RUE calibration
    if np.isfinite(LAI_proxy):
        LAI_used = np.mean([LAI_mean, LAI_proxy])
    else:
        LAI_used = LAI_mean

    # RUE_max curve adjusted for LAI
    RUE_max_effective = RUE_min_value + (RUE_max_base_value - RUE_min_value) * (1 - math.exp(-RUE_ALPHA * LAI_used))

    # Fraction of PAR intercepted by canopy at season level
    fpar_season = 1 - math.exp(-k_def_value * LAI_used)

    # Intercepted PAR (MJ PAR/m2/day)
    if PARs > 0:
        IPAR_season = PARs * fpar_season
    else:
        IPAR_season = 0

    # Convert yield (t/ha) into dry matter index used for RUE calibration.
    DM_total = yield_t_ha * 100

    # Observed RUE from yield and IPAR
    if DM_total > 0 and IPAR_season > 0:
        RUE_empirical = DM_total / IPAR_season
    else:
        RUE_empirical = float("nan")

    # Final corrected maximum RUE, bounded between RUE_min and effective RUE_max
    if np.isfinite(RUE_empirical):
        RUE_max = min(max(RUE_empirical, RUE_min_value), RUE_max_effective)
    else:
        RUE_max = (RUE_min_value + RUE_max_base_value) / 2

    # fallback for RUE_max (1.45)
    if not isinstance(RUE_max, (float, int)) or np.isnan(RUE_max) or RUE_max <= 0:
        RUE_max = 1.45

    # -------------------------------------------------------------
    # RUE_ASYMPTOTE_FRAC CALCULATION
    # -------------------------------------------------------------

    DEFAULT_KEXT_VALUE = 0.6         # Fallback canopy extinction coefficient
    ASYMP_BASE_FRAC = 0.86           # Base asymptotic RUE fraction
    ASYMP_SPAN_FRAC = 0.09           # Span between base and maximum asymptotic fraction
    ASYMP_MIN_CLIP = 0.8             # Lower bound of asymptotic fraction
    ASYMP_MAX_CLIP = 0.95            # Upper bound of asymptotic fraction

    # CATEGORY fPAR_target
    fPAR_target_map = {
        "cereal": 0.93,
        "leaf vegetables": 0.95,
        "legume vegetables": 0.92,
        "root vegetables": 0.9,
        "bulb vegetables": 0.9,
        "stem vegetables": 0.9,
        "fruit": 0.92,
        "fruit vegetables": 0.92,
        "oil seed": 0.9,
        "legume oil seed crop": 0.9,
        "fiber crop": 0.88,
    }

    # fPAR target depends on crop category
    fPAR_target = fPAR_target_map.get(crop_category, 0.92)

    # Compute k_ext
    if max_lai > 0:
        k_ext_target = -math.log(1 - fPAR_target) / max_lai
    else:
        k_ext_target = DEFAULT_KEXT_VALUE

    s_value = k_ext_target * LAI_used

    # Category gamma 
    gamma_map = {
        "cereal": 0.9,
        "leaf vegetables": 1.2,
        "legume vegetables": 0.7,
        "root vegetables": 0.9,
        "fruit": 1.0,
        "oil seed": 0.9,
        "fiber crop": 0.9,
    }

    gamma_value = gamma_map.get(crop_category, 0.9)

    # Compute A_frac_raw
    A_frac_raw = ASYMP_BASE_FRAC + ASYMP_SPAN_FRAC * (1 - math.exp(-gamma_value * s_value))

    # Final A_frac
    if (isinstance(A_frac_raw, (int, float)) and RUE_max > 0):
        A_frac = A_frac_raw
    else:
        A_frac = ASYMP_BASE_FRAC

    RUE_asymptote_frac = max(ASYMP_MIN_CLIP, min(A_frac, ASYMP_MAX_CLIP))

    # fallback for AsymFrac (0.90)
    if (not isinstance(RUE_asymptote_frac, (float, int))
        or np.isnan(RUE_asymptote_frac)
        or RUE_asymptote_frac <= 0):
        RUE_asymptote_frac = 0.90

    # -------------------------------------------------------------
    # RUE_LAI_HALF_SAT CALCULATION
    # -------------------------------------------------------------

    K0_FALLBACK = 2.5         # Default midpoint LAI if max_lai missing
    BETA_DEFAULT = 1.2        # Logistic curve steepness
    S0_DEFAULT = 2.5          # Logistic curve shift
    POS_AMP = 0.25            # Positive adjustment amplitude
    NEG_AMP = -0.5            # Negative adjustment amplitude
    K_MIN_CLIP = 0.8          # Minimum allowed RUE_LAI_Half_Sat

    # Compute K0 (Excel: LAIm/2 or fallback 2.5) 
    if max_lai > 0:
        K0_value = max_lai / 2
    else:
        K0_value = K0_FALLBACK

    # Logistic function 
    logistic_value = 1 / (1 + math.exp(-BETA_DEFAULT * (s_value - S0_DEFAULT)))

    Kadj_raw = POS_AMP + (NEG_AMP - POS_AMP) * logistic_value

    # Final K value before clipping 
    K_raw = K0_value + Kadj_raw

    RUE_LAI_half_sat = max(K_MIN_CLIP, K_raw)

    # Excel fallback for HalfSat (2.50)
    if (not isinstance(RUE_LAI_half_sat, (float, int))
        or np.isnan(RUE_LAI_half_sat)
        or RUE_LAI_half_sat <= 0):
        RUE_LAI_half_sat = 2.50

    # -------------------------------------------------------------
    # RUE_DAILY_DELTA_MAX CALCULATION
    # -------------------------------------------------------------

    BETA_K = 1.2                  # Logistic slope for K adjustment
    S0_K = 2.5                    # Logistic shift for K adjustment
    POS_AMP_K = 0.25              # Positive amplitude for K logistic
    NEG_AMP_K = -0.5              # Negative amplitude for K logistic
    K_MIN_CLIP = 0.8              # Minimum allowed K (LAI half saturation)

    GAMMA_RAF = 0.9               # Gamma for RAF smoothing
    ASYMP_BASE_FRAC = 0.86        # Asymptote base fraction
    ASYMP_SPAN_FRAC = 0.09        # Asymptote span
    RAF_MIN_CLIP = 0.8            # Min cap for asymptote fraction smoothing
    RAF_MAX_CLIP = 0.95           # Max cap for asymptote fraction smoothing

    GROWTH_BASE_SCALE = 1.0       # Scaling factor for baseFrac
    GROWTH_AMP = 0.6              # Amplitude for growth fraction
    FRAC_DYNAMIC_MAX = 0.12       # Maximum dynamic fraction cap
    DELTA_MIN_CLIP = 0.01         # Minimum allowed RUE delta

    # CATEGORY BASE FRACTION 
    base_frac_category_map = {
        "cereal": 0.04,
        "legume vegetables": 0.05,
        "leaf vegetables": 0.06,
        "root vegetables": 0.05,
        "bulb vegetables": 0.05,
        "stem vegetables": 0.05,
        "fruit": 0.05,
        "fruit vegetables": 0.05,
        "oil seed": 0.04,
        "legume oil seed crop": 0.04,
        "fiber crop": 0.04,
    }

    base_frac_cat = base_frac_category_map.get(crop_category, 0.04)
    base_frac = base_frac_cat * GROWTH_BASE_SCALE

    # K_smooth vs K_raw logic 
    if max_lai > 0:
        K_proxy = max_lai / 2
    else:
        K_proxy = 2.5

    logistic_k = 1 / (1 + math.exp(-BETA_K * (s_value - S0_K)))
    Kadj_smooth = POS_AMP_K + (NEG_AMP_K - POS_AMP_K) * logistic_k
    K_smooth_raw = K_proxy + Kadj_smooth

    # Final K
    K_final_value = RUE_LAI_half_sat
    if not isinstance(K_final_value, (float, int)):
        K_final_value = K_smooth_raw

    K_final_value = max(K_MIN_CLIP, K_final_value)

    # RAF smoothing logic 
    RAF_smooth_raw = ASYMP_BASE_FRAC +ASYMP_SPAN_FRAC * (1 - math.exp(-GAMMA_RAF * s_value))

    if isinstance(RAF_smooth_raw, (float, int)):
        RAF_smooth = RAF_smooth_raw
    else:
        RAF_smooth = 0.9

    if isinstance(RUE_asymptote_frac, (float, int)):
        RAF_used = RUE_asymptote_frac
    else:
        RAF_used = max(RAF_MIN_CLIP, min(RAF_smooth, RAF_MAX_CLIP))

    # Growth fraction 
    if K_final_value > 0:
        growth_frac_raw = (K_final_value - LAI_used) / K_final_value
    else:
        growth_frac_raw = 0

    growth_frac = max(0, min(1, growth_frac_raw))

    # Dynamic fraction 
    frac_dynamic = base_frac * (1 + GROWTH_AMP * growth_frac)
    frac_capped = min(frac_dynamic, FRAC_DYNAMIC_MAX)

    # Final Delta 
    delta_raw = RUE_max * frac_capped * RAF_used
    RUE_daily_delta_max = max(DELTA_MIN_CLIP, delta_raw)

    # -------------------------------------------------------------
    # Temperature Stress (Excel-style Type + Intensity)
    # -------------------------------------------------------------

    temp_stress_type_list = []        # "Cold", "Heat", "None"
    temp_stress_intensity_list = []   # numeric degrees deviation

    for Ta in avg_temp:

        Tb = crop_base_T
        Topt = crop_opt_T

        cold_deg = max(0, Tb - Ta)
        heat_deg = max(0, Ta - Topt)

        if cold_deg > 0:
            stress_type = "Cold"
            stress_intensity = round(cold_deg, 1)
        elif heat_deg > 0:
            stress_type = "Heat"
            stress_intensity = round(heat_deg, 1)
        else:
            stress_type = "None"
            stress_intensity = 0.0

        temp_stress_type_list.append(stress_type)
        temp_stress_intensity_list.append(stress_intensity)

    # -------------------------------------------------------------
    # TEMPERATURE STRESS MODULE (Ts_eff for RUE adjustment)
    # -------------------------------------------------------------

    temp_stress_cold_factor = []
    temp_stress_heat_factor = []
    temp_stress_raw_factor = []
    temp_stress_effective_factor = []

    for Ta in avg_temp:
        # Cold stress factor (0–1)
        if crop_opt_T > crop_base_T:
            cold = (Ta - crop_base_T) / (crop_opt_T - crop_base_T)
            cold = max(0.0, min(1.0, cold))
        else:
            cold = 0.0

        # Heat stress factor (0–1)
        if crop_upper_T > crop_opt_T:
            heat = (crop_upper_T - Ta) / (crop_upper_T - crop_opt_T)
            heat = max(0.0, min(1.0, heat))
        else:
            heat = 0.0

        # Raw stress factor (minimum of cold, heat, STI)
        ts_raw = min(cold, heat, stress_tolerance_index)

        # Effective stress factor: full STI within temperature window, else raw
        if (Ta >= crop_base_T) and (Ta <= crop_upper_T):
            ts_eff = stress_tolerance_index
        else:
            ts_eff = ts_raw

        temp_stress_cold_factor.append(round(cold, 4))
        temp_stress_heat_factor.append(round(heat, 4))
        temp_stress_raw_factor.append(round(ts_raw, 4))
        temp_stress_effective_factor.append(round(ts_eff, 4))

    # -------------------------------------------------------------
    # RUE RESPONSE CURVE (Daily, LAI & Stress Adjusted)
    # -------------------------------------------------------------

    rue_base_list = []
    rue_limited_list = []
    rue_adjusted_list = []

    for i in range(len(dates)):
        if i < len(lai_list):
            lai_today = lai_list[i]
        else:
            lai_today = 0.0
            
        if lai_today is None or lai_today <= 0 or (RUE_LAI_half_sat + lai_today) <= 0:
            rue_base = 0.0
        else:
            rue_base = (RUE_max * RUE_asymptote_frac) * lai_today / (RUE_LAI_half_sat + lai_today)
    
        # Upper bound at RUE_max
        rue_limited = min(rue_base, RUE_max)

        # Apply temperature stress factor
        if i < len(temp_stress_effective_factor):
            ts_eff_today = temp_stress_effective_factor[i]
        else:
            ts_eff_today = 0.0

        rue_adjusted = rue_limited * ts_eff_today

        rue_base_list.append(round(rue_base, 4))
        rue_limited_list.append(round(rue_limited, 4))
        rue_adjusted_list.append(round(rue_adjusted, 4))

    # -------------------------------------------------------------
    # DAILY BIOMASS (PAR × RUE × NPK × WATER)
    # -------------------------------------------------------------

    par_intercepted_list = []
    biomass_raw_list = []
    biomass_adjusted_list = []

    for i in range(len(DttofStages)):
        # Skip if no principal stage (no active crop canopy)
        principal = DttofStages[i]["principal_stage"]
        if not principal:
            par_intercepted_list.append(0.0)
            biomass_raw_list.append(0.0)
            biomass_adjusted_list.append(0.0)
            continue

        # Radiation and interception
        if i < len(global_solar_radiation_rs):
            rg_today = float(global_solar_radiation_rs[i])
        else:
            rg_today = 0.0

        if i < len(fpar_list):
            fpar_today = fpar_list[i]
        else:
            fpar_today = 0.0

        if i < len(rue_adjusted_list):
            rue_adj_today = rue_adjusted_list[i]
        else:
            rue_adj_today = 0.0

        par_intercepted = rg_today * fpar_today  # MJ/m²/day PAR intercepted
        biomass_raw = par_intercepted * rue_adj_today * 10

        # Efficiencies
        if i < len(NPK_efficiency):
            npk_eff_today = NPK_efficiency[i]
        else:
            npk_eff_today = 0.0

        if i < len(water_efficiency):
            water_eff_today = water_efficiency[i]
            if water_eff_today == "":
                water_eff_today = 0.0
        else:
            water_eff_today = 0.0

        biomass_adjusted = biomass_raw * npk_eff_today * water_eff_today

        par_intercepted_list.append(round(par_intercepted, 4))
        biomass_raw_list.append(round(biomass_raw, 4))
        biomass_adjusted_list.append(round(biomass_adjusted, 4))

    # -------------------------------------------------------------
    # DM / FM ADJUSTMENT & DAILY DM
    # -------------------------------------------------------------

    bbch_value_list = []
    fruiting_conversion_factor_list = []
    daily_dm_list = []

    # Normalize dm_fm_ratio
    if dm_fm_ratio > 1:
        dm_fm_ratio_normalized = dm_fm_ratio / 100.0
    else:
        dm_fm_ratio_normalized = dm_fm_ratio

    for i in range(len(DttofStages)):
        stg = DttofStages[i]
        main_stage = str(stg.get("main_stage", "") or "").strip().lower()
        principal_stage = str(stg.get("principal_stage", "") or "").strip().lower()
        sub_stage_key = stg.get("sub_stage", "")

        if sub_stage_key in cropSubStages:
            bbch_val = extract_numeric(str(sub_stage_key).split(":")[0], default=0.0)
        else:
            bbch_val = 0.0

        bbch_value_list.append(round(bbch_val, 2))

        if "senescence" in (main_stage, principal_stage):
            factor = 0.0
        elif bbch_val >= fruiting_start_bbch and dm_fm_ratio_normalized > 0:
            factor = dm_fm_ratio_normalized
        else:
            factor = 0.0
        
        fruiting_conversion_factor_list.append(round(factor, 4))

        # Final daily dry matter (DM)
        if i < len(biomass_adjusted_list):
            bm_adj_today = biomass_adjusted_list[i]
        else:
            bm_adj_today = 0.0

        if bm_adj_today > 0:
            # dm_today = bm_adj_today * (1 + factor)
            dm_today = round(bm_adj_today, 2) * (1 + factor)
        else:
            dm_today = 0.0

        daily_dm_list.append(dm_today)

    # -------------------------------------------------------------
    # CUMULATIVE DM
    # -------------------------------------------------------------

    cumulative_dm_list = []
    running_dm_sum = 0.0

    for dm in daily_dm_list:
        running_dm_sum += dm
        cumulative_dm_list.append(round(running_dm_sum, 4))

    # -------------------------------------------------------------
    # TOTAL BIOMASS (Season Accumulation Up To Current Day)
    # -------------------------------------------------------------

    total_biomass_list = []
    cumulative_total = 0.0

    for i in range(len(DttofStages)):
        principal = DttofStages[i]["principal_stage"]
        
        if not principal:
            total_biomass_list.append(0.0)
            continue

        if i < len(daily_dm_list):
            dm_val = daily_dm_list[i]
        else:
            dm_val = 0.0

        cumulative_total += dm_val
        total_biomass_list.append(round(cumulative_total, 4))

    # -------------------------------------------------------------
    # YIELD CALCULATION (Final Yield = Total_BM × HI)
    # -------------------------------------------------------------

    yield_list = []

    # Normalize harvest_index: if >1 treat as percent, else as fraction
    if harvest_index > 1:
        hi_value = harvest_index / 100.0
    else:
        hi_value = harvest_index
    
    for i in range(len(DttofStages)):
        principal = DttofStages[i]["principal_stage"]

        if not principal:
            yield_list.append(0.0)
            continue

        # Total_BM for current day
        if i < len(total_biomass_list):
            cum_bm_today = total_biomass_list[i]
        else:
            cum_bm_today = 0.0

        # Final yield for this day
        yield_today = cum_bm_today * hi_value
        yield_list.append(yield_today)

    # -------------------------------------------------------------
    # FINAL TOTAL YIELD (full-season yield)
    # -------------------------------------------------------------
    try:
        total_yield_final = yield_list[-1]   # last day of simulation
    except:
        total_yield_final = 0

    # -------------------------------------------------------------
    # FINAL DAILY SUMMARY (Simulator Output)
    # -------------------------------------------------------------

    today_index = None
    
    # Locate today's day in DttofStages
    for i, stg in enumerate(DttofStages):
        try:
            stg_date = pd.to_datetime(stg["date"]).date()
        except:
            continue

        if stg_date == TODAY:
            today_index = i
            break

    # If today not found, pick the nearest future/backward date
    if today_index is None:
        dates_only = [pd.to_datetime(d).date() for d in final["date"]]
        diffs = [abs((d - TODAY).days) for d in dates_only]
        today_index = int(np.argmin(diffs))

    # Extract today's stage info
    stage_today = DttofStages[today_index]
    main_stage_today = stage_today.get("main_stage", "")
    principal_stage_today = stage_today.get("principal_stage", "")
    sub_stage_today = stage_today.get("sub_stage", "")
    bbch_today = stage_today.get("bbch_scale", 0)
    crop_age_today = (TODAY - sow_date).days
  
    # Weather today
    if today_index < len(avg_temp):
        temp_today = avg_temp[today_index]
    else:
        temp_today = 0

    if today_index < len(precipitation_adjusted):
        rain_today = precipitation_adjusted[today_index]
    else:
        rain_today = 0

    if today_index < len(global_solar_radiation_rs):
        radiation_today = global_solar_radiation_rs[today_index]
    else:
        radiation_today = 0

    if today_index < len(relative_humidity_2m_max):
        humidity_today = relative_humidity_2m_max[today_index]
    else:
        humidity_today = 0

    if today_index < len(windspeed_10m_max):
        wind_today = windspeed_10m_max[today_index]
    else:
        wind_today = 0

    # Irrigation today
    if today_index < len(needed):
        irrigation_required_today = needed[today_index]
    else:
        irrigation_required_today = ""

    if today_index < len(irrig):
        irrigation_amount_today = irrig[today_index]
    else:
        irrigation_amount_today = 0

    # LAI, fPAR, RUE today
    if today_index < len(lai_list):
        lai_today = lai_list[today_index]
    else:
        lai_today = 0

    if today_index < len(fpar_list):
        fpar_today = fpar_list[today_index]
    else:
        fpar_today = 0

    if today_index < len(rue_adjusted_list):
        rue_today = rue_adjusted_list[today_index]
    else:
        rue_today = 0

    if today_index < len(par_intercepted_list):
        par_today = par_intercepted_list[today_index]
    else:
        par_today = 0

    # Biomass & DM today
    if today_index < len(daily_dm_list):
        dm_today = daily_dm_list[today_index]
    else:
        dm_today = 0

    if today_index < len(total_biomass_list):
        total_biomass_today = total_biomass_list[today_index]
    else:
        total_biomass_today = 0

    if today_index < len(yield_list):
        yield_today = yield_list[today_index]
    else:
        yield_today = 0

    # GDD today (use adjusted GDD now)
    if today_index < len(final["adjusted_gdd"]):
        gdd_today = final["adjusted_gdd"].iloc[today_index]
    else:
        gdd_today = 0

    # Cumulative GDD today (already based on adjusted_gdd)
    if today_index < len(cumulative_gdd):
        cumulative_gdd_today = cumulative_gdd[today_index]
    else:
        cumulative_gdd_today = 0

    simulator_data = {
        "farmUid": farm_id,
        "gridId": grid,
        "userUid": userUid,

        # Crop age & stage info
        "is_crop_active": crop_active,
        "crop_age": crop_age_today,
        "main_stage": main_stage_today,
        "principal_stage": principal_stage_today,
        "sub_stage": sub_stage_today,
        "bbch": bbch_today,

        # Weather summary (rounded)
        "temperature_today": round(temp_today, 2),
        "radiation_today": round(radiation_today, 2),
        "rainfall_today": round(rain_today, 2),
        "humidity_today": round(humidity_today, 2),
        "wind_today": round(wind_today, 2),

        # Water/irrigation
        "irrigation_required_today": irrigation_required_today,
        "irrigation_amount_today": round(irrigation_amount_today, 2),

        # Plant physiological indicators
        "lai_today": round(lai_today, 3),
        "fpar_today": round(fpar_today, 4),
        "rue_today": round(rue_today, 3),
        "par_intercepted_today": round(par_today, 2),

        # Biomass, DM, Yield
        "dm_today": round(dm_today, 2),
        "total_biomass_today": round(total_biomass_today, 2),
        "yield_today": round(yield_today, 2),
        "total_yield": round(total_yield_final, 2),

        # Daily GDD (adjusted), Cum. GDD (adjusted)
        "gdd_today": round(gdd_today, 2),
        "cumulative_gdd_today": round(cumulative_gdd_today, 2),

        # Temperature stress type, intensity
        "temp_stress_type_today": temp_stress_type_list[today_index],
        "temp_stress_intensity_today": round(temp_stress_intensity_list[today_index], 2),
    }

    return simulator_data
