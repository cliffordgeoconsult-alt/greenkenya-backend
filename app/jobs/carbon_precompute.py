# This script precomputes carbon stats for all counties for a given year and saves them to the database.
# It can be run as a standalone job or integrated into a scheduler for regular updates.
# Usage: python app/jobs/carbon_precompute.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, UTC
from app.services.gee.ee_init import initialize_ee
from app.db.session import SessionLocal
from concurrent.futures import ThreadPoolExecutor

from app.services.carbon_service import (
    fetch_counties,
    fetch_wards,
    fetch_reserves,
    get_single_county_carbon,
    get_single_ward_carbon,
    get_single_reserve_carbon,
    get_single_county_loss,
    get_single_ward_loss,
    get_single_reserve_loss
)

def save_carbon(db, entity_type, entity_id, name, year, data):
    db.execute(
        text("""
        INSERT INTO carbon_stats (
            entity_type, entity_id, name, year,
            dense_forest_ha, tree_cover_ha,
            biomass_tonnes, carbon_tonnes,
            co2e_tonnes, carbon_density
        )
        VALUES (
            :entity_type, :entity_id, :name, :year,
            :dense_forest_ha, :tree_cover_ha,
            :biomass_tonnes, :carbon_tonnes,
            :co2e_tonnes, :carbon_density
        )
        ON CONFLICT (entity_type, entity_id, year)
        DO UPDATE SET
            dense_forest_ha = EXCLUDED.dense_forest_ha,
            tree_cover_ha = EXCLUDED.tree_cover_ha,
            biomass_tonnes = EXCLUDED.biomass_tonnes,
            carbon_tonnes = EXCLUDED.carbon_tonnes,
            co2e_tonnes = EXCLUDED.co2e_tonnes,
            carbon_density = EXCLUDED.carbon_density;
        """),
        {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "name": name,
            "year": year,
            "dense_forest_ha": data["dense_forest_ha"],
            "tree_cover_ha": data["tree_cover_ha"],
            "biomass_tonnes": data["biomass_tonnes"],
            "carbon_tonnes": data["carbon_tonnes"],
            "co2e_tonnes": data["co2e_tonnes"],
            "carbon_density": data["carbon_density_tco2e_ha"],
        }
    )

def save_loss(db, entity_type, entity_id, name, year, data):
    db.execute(
        text("""
        INSERT INTO loss_stats (
            entity_type, entity_id, name, year,
            loss_ha, biomass_lost_tonnes,
            carbon_lost_tonnes, co2e_emitted_tonnes
        )
        VALUES (
            :entity_type, :entity_id, :name, :year,
            :loss_ha, :biomass_lost_tonnes,
            :carbon_lost_tonnes, :co2e_emitted_tonnes
        )
        ON CONFLICT (entity_type, entity_id, year)
        DO UPDATE SET
            loss_ha = EXCLUDED.loss_ha,
            biomass_lost_tonnes = EXCLUDED.biomass_lost_tonnes,
            carbon_lost_tonnes = EXCLUDED.carbon_lost_tonnes,
            co2e_emitted_tonnes = EXCLUDED.co2e_emitted_tonnes;
        """),
        {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "name": name,
            "year": year,
            "loss_ha": data["loss_ha"],
            "biomass_lost_tonnes": data["biomass_lost_tonnes"],
            "carbon_lost_tonnes": data["carbon_lost_tonnes"],
            "co2e_emitted_tonnes": data["co2e_emitted_tonnes"],
        }
    )

def run_carbon_precompute(year: int):
    db: Session = SessionLocal()
    print("🔥 STARTING JOB")

    initialize_ee()

    try:
        TARGET_COUNTIES = [
            "NAIROBI",
            "NAKURU",
            "MOMBASA",
            "KISUMU",
            "NYERI",
            "NAROK",
            "TAITA TAVETA",
            "TANA RIVER",
            "KISII"
        ]

        all_counties = fetch_counties(db)

        counties = [
            c for c in all_counties
            if c.name.upper() in TARGET_COUNTIES
        ]
        all_wards = fetch_wards(db)

        county_ids = {c.id for c in counties}

        wards = [
            w for w in all_wards
            if w.county_id in county_ids
        ]
        reserves = fetch_reserves(db)

        def process_county(county):
            print(f"⏳ [{year}] County: {county.name}")
            local_db = SessionLocal()
            try:
                carbon = get_single_county_carbon(local_db, str(county.id), year)
                loss = get_single_county_loss(local_db, str(county.id), year)

                if "error" not in carbon:
                    save_carbon(local_db, "county", str(county.id), county.name, year, carbon)

                if "error" not in loss:
                    save_loss(local_db, "county", str(county.id), county.name, year, loss)

                local_db.commit()
                print(f"✅ County done: {county.name}")
            finally:
                local_db.close()

        def process_ward(ward):
            print(f"⏳ [{year}] Ward: {ward.name}")
            local_db = SessionLocal()
            try:
                carbon = get_single_ward_carbon(local_db, str(ward.id), year)
                loss = get_single_ward_loss(local_db, str(ward.id), year)

                if "error" not in carbon:
                    save_carbon(local_db, "ward", str(ward.id), ward.name, year, carbon)

                if "error" not in loss:
                    save_loss(local_db, "ward", str(ward.id), ward.name, year, loss)

                local_db.commit()
                print(f"✅ Ward done: {ward.name}")
            finally:
                local_db.close()

        def process_reserve(reserve):
            print(f"⏳ [{year}] Reserve: {reserve.name}")
            local_db = SessionLocal()
            try:
                carbon = get_single_reserve_carbon(local_db, str(reserve.reserve_id), year)
                loss = get_single_reserve_loss(local_db, str(reserve.reserve_id), year)

                if "error" not in carbon:
                    save_carbon(local_db, "reserve", str(reserve.reserve_id), reserve.name, year, carbon)

                if "error" not in loss:
                    save_loss(local_db, "reserve", str(reserve.reserve_id), reserve.name, year, loss)

                local_db.commit()
                print(f"✅ Reserve done: {reserve.name}")
            finally:
                local_db.close()
        # 🚀 PARALLEL EXECUTION
        tasks = []

        for c in counties:
            tasks.append(("county", c))

        for w in wards:
            tasks.append(("ward", w))

        for r in reserves:
            tasks.append(("reserve", r))

        def process(task):
            kind, obj = task

            if kind == "county":
                process_county(obj)
            elif kind == "ward":
                process_ward(obj)
            elif kind == "reserve":
                process_reserve(obj)

        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(process, tasks)

        print("🔥 ALL AOIs carbon precompute done")

    finally:
        db.close()

def run_loss_only_precompute(year: int):
    db: Session = SessionLocal()
    print(f"🔥 STARTING LOSS-ONLY JOB {year}")

    initialize_ee()

    try:
        TARGET_COUNTIES = [
            "NAIROBI",
            "NAKURU",
            "MOMBASA",
            "KISUMU",
            "NYERI",
            "NAROK",
            "TAITA TAVETA",
            "TANA RIVER",
            "KISII"
        ]

        all_counties = fetch_counties(db)

        counties = [
            c for c in all_counties
            if c.name.upper() in TARGET_COUNTIES
        ]

        all_wards = fetch_wards(db)
        county_ids = {c.id for c in counties}

        wards = [
            w for w in all_wards
            if w.county_id in county_ids
        ]

        reserves = fetch_reserves(db)

        def process_county(county):
            print(f"⏳ [{year}] County: {county.name}")
            local_db = SessionLocal()
            try:
                loss = get_single_county_loss(local_db, str(county.id), year)

                if "error" not in loss:
                    save_loss(local_db, "county", str(county.id), county.name, year, loss)

                local_db.commit()
            finally:
                local_db.close()

        def process_ward(ward):
            local_db = SessionLocal()
            try:
                loss = get_single_ward_loss(local_db, str(ward.id), year)

                if "error" not in loss:
                    save_loss(local_db, "ward", str(ward.id), ward.name, year, loss)

                local_db.commit()
            finally:
                local_db.close()

        def process_reserve(reserve):
            local_db = SessionLocal()
            try:
                loss = get_single_reserve_loss(local_db, str(reserve.reserve_id), year)

                if "error" not in loss:
                    save_loss(local_db, "reserve", str(reserve.reserve_id), reserve.name, year, loss)

                local_db.commit()
            finally:
                local_db.close()

        tasks = []

        for c in counties:
            tasks.append(("county", c))
        for w in wards:
            tasks.append(("ward", w))
        for r in reserves:
            tasks.append(("reserve", r))

        def process(task):
            kind, obj = task

            if kind == "county":
                process_county(obj)
            elif kind == "ward":
                process_ward(obj)
            elif kind == "reserve":
                process_reserve(obj)

        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.map(process, tasks)

        print(f"🔥 LOSS-ONLY DONE {year}")

    finally:
        db.close()

def get_last_completed_year(db, table, entity_type):
    result = db.execute(
        text(f"""
            SELECT MAX(year) as max_year
            FROM {table}
            WHERE entity_type = :entity_type
        """),
        {"entity_type": entity_type}
    )

    row = result.fetchone()
    return row.max_year if row and row.max_year else None

if __name__ == "__main__":
    from app.services.carbon_service import (
        CARBON_START_YEAR,
        LOSS_START_YEAR,
        CURRENT_OFFICIAL_YEAR
    )

    db = SessionLocal()

    try:
        # 🔍 CHECK LAST COMPLETED YEARS
        last_loss_year = get_last_completed_year(db, "loss_stats", "county")
        last_carbon_year = get_last_completed_year(db, "carbon_stats", "county")

        print(f"📊 Last loss year in DB: {last_loss_year}")
        print(f"📊 Last carbon year in DB: {last_carbon_year}")

        # 🔥 1. LOSS HISTORY (resume)
        start_loss_year = (last_loss_year + 1) if last_loss_year else LOSS_START_YEAR

        for year in range(start_loss_year, CARBON_START_YEAR):
            print(f"\n▶ Resuming LOSS precompute for year {year}")
            run_loss_only_precompute(year)

        # 🔥 2. CARBON + LOSS (resume)
        start_carbon_year = (
            max(CARBON_START_YEAR, (last_carbon_year + 1) if last_carbon_year else CARBON_START_YEAR)
        )

        for year in range(start_carbon_year, CURRENT_OFFICIAL_YEAR + 1):
            print(f"\n▶ Resuming CARBON + LOSS precompute for year {year}")
            run_carbon_precompute(year)

    finally:
        db.close()