from fastapi import FastAPI, BackgroundTasks
import meltano.core.tracking
from meltano.core.project import Project
from meltano.core.tracking.contexts import CliContext
import os
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = sa.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    id = sa.Column(sa.Integer, primary_key=True)
    start_time = sa.Column(sa.DateTime, default=datetime.utcnow)
    status = sa.Column(sa.String)

# Create tables on startup
Base.metadata.create_all(bind=engine)

async def run_pipeline_task(run_id: int):
    db = SessionLocal()
    try:
        logger.info(f"Starting pipeline run {run_id}")
        
        project = Project.find()
        meltano.core.tracking.DISABLED = True
        
        context = CliContext(
            project=project,
            command="elt",
            command_args=["tap-csv", "target-postgres", "--job_id", str(run_id)]
        )
        
        pipeline_run = db.query(PipelineRun).get(run_id)
        pipeline_run.status = "completed"
        logger.info(f"Pipeline run {run_id} completed successfully")
        
        db.commit()
    except Exception as e:
        logger.error(f"Pipeline run {run_id} failed: {str(e)}")
        pipeline_run = db.query(PipelineRun).get(run_id)
        pipeline_run.status = "failed"
        db.commit()
        raise e
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "Meltano Pipeline Service", "status": "running"}

@app.post("/run")
async def run_pipeline(background_tasks: BackgroundTasks):
    db = SessionLocal()
    try:
        pipeline_run = PipelineRun(status="started")
        db.add(pipeline_run)
        db.commit()
        db.refresh(pipeline_run)
        
        logger.info(f"Created new pipeline run with ID {pipeline_run.id}")
        background_tasks.add_task(run_pipeline_task, pipeline_run.id)
        
        return {
            "message": "Pipeline started",
            "run_id": pipeline_run.id
        }
    finally:
        db.close()

@app.get("/status/{run_id}")
async def get_status(run_id: int):
    db = SessionLocal()
    try:
        pipeline_run = db.query(PipelineRun).get(run_id)
        if not pipeline_run:
            return {"error": "Run not found"}
            
        return {
            "run_id": pipeline_run.id,
            "status": pipeline_run.status,
            "start_time": pipeline_run.start_time
        }
    finally:
        db.close()
