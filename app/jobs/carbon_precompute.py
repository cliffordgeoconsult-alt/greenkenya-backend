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
        counties = fetch_counties(db)
        wards = fetch_wards(db)
        reserves = fetch_reserves(db)

        def process_county(county):
            print(f"⏳ County starting: {county.name}")
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
            print(f"⏳ Ward starting: {ward.name}")
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
            print(f"⏳ Reserve starting: {reserve.name}")
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

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(process, tasks)

        print("🔥 ALL AOIs carbon precompute done")

    finally:
        db.close()
if __name__ == "__main__":
    year = datetime.now(UTC).year - 1
    print(f"🚀 Running carbon precompute for year {year}")
    run_carbon_precompute(year)