from app.db.session import engine
from app.db.base import Base

from app.models.forest import Forest
from app.models.county import County

print("Creating tables...")

Base.metadata.create_all(bind=engine)

print("Tables created.")