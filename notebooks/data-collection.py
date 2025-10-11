import fastf1
import pandas as pd
from datetime import datetime
import os
import time

#--- Configure Cache ---
fastf1.Cache.enable_cache('cache')

#--- Configure the directory to save data
processed_data_dir = 'data/processed'

#--- Data Collection ---
start_year = 2018
current_year = datetime.now().year

# Lists to store dataframes from all events
all_race_results = []
all_lap_data = []
all_qualifying_results = []
all_practice_results = []
all_pit_stop_data = []
all_weather_data = []

for year in range(start_year, current_year + 1):
    print(f"\n{'='*20} \nProcessing season: {year}...")
    schedule = fastf1.get_event_schedule(year)

    #Only get data for races that have already happened
    races_to_process = schedule[schedule['EventDate'] < datetime.now()]

    for index, race_info in races_to_process.iterrows():
        #Skip all unofficial races
        if race_info['RoundNumber'] == 0:
            continue

        event_name = race_info['EventName']
        round_number = race_info['RoundNumber']
        print(f"  -> Fetching data for: {event_name} (Round {round_number})")

        try:
            # Load the main race data
            session_race = fastf1.get_session(year, round_number, 'R')
            session_race.load(laps=True, telemetry=False, weather=True, messages=False)

            # 1. Race Results
            results = session_race.results
            results['Year'] = year
            results['RaceName'] = event_name
            all_race_results.append(results)

            # 2. Lap Data
            laps = session_race.laps
            laps['Year'] = year
            laps['RaceName'] = event_name
            all_lap_data.append(laps)

            # 3. Pit Stop Data
            pit_stops = session_race.laps[pd.notna(session_race.laps['PitInTime'])]
            if not pit_stops.empty:
                pit_stops = pit_stops.copy() # Sử dụng .copy() để tránh cảnh báo
                pit_stops['Year'] = year
                pit_stops['RaceName'] = event_name
                all_pit_stop_data.append(pit_stops)

            # 4. Weather Data
            weather = session_race.weather_data
            weather['Year'] = year
            weather['RaceName'] = event_name
            all_weather_data.append(weather)

            # --- Qualifying Data ---
            session_quali = fastf1.get_session(year, round_number, 'Q')
            session_quali.load(laps=False, telemetry=False, weather=False, messages=False)
            quali_results = session_quali.results
            quali_results['Year'] = year
            quali_results['RaceName'] = event_name
            all_qualifying_results.append(quali_results)

            # Practice Data
            for practice_session_name in ['FP1', 'FP2', 'FP3']:
                try:
                    session_practice = fastf1.get_session(year, round_number, practice_session_name)
                    session_practice.load(laps=True, telemetry=False, weather=False, messages=False)
                    practice_laps = session_practice.laps.copy()
                    practice_laps['Session'] = practice_session_name
                    practice_laps['Year'] = year
                    practice_laps['RaceName'] = event_name
                    all_practice_results.append(practice_laps)
                except Exception as e:
                    print(f"    [!] No data for {practice_session_name} in {event_name}. Skipping. Error: {e}")

            time.sleep(2)

        except Exception as e:
            print(f"    [X] CRITICAL ERROR loading main session for {event_name}. Skipping this event. Error: {e}")
            continue

print("\nData collection complete. Consolidating and saving all files...")

#Save Data
def save_data(data_list, filename):
    if data_list:
        df = pd.concat(data_list)
        df.to_csv(f"{processed_data_dir}/{filename}", index=False)
        print(f"Successfully saved: {filename}")
    else:
        print(f"No data to save for: {filename}")

save_data(all_race_results, f"race_results_{start_year}_to_{current_year}.csv")
save_data(all_lap_data, f"laps_data_{start_year}_to_{current_year}.csv")
save_data(all_qualifying_results, f"qualifying_results_{start_year}_to_{current_year}.csv")
save_data(all_practice_results, f"practice_laps_{start_year}_to_{current_year}.csv")
save_data(all_pit_stop_data, f"pit_stops_{start_year}_to_{current_year}.csv")
save_data(all_weather_data, f"weather_data_{start_year}_to_{current_year}.csv")

print(f"\nAll data has been saved to the '{processed_data_dir}' folder.")