from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
import meltano.core.tracking
from meltano.core.project import Project
from meltano.core.tracking.contexts import CliContext
from meltano.core.runner import CliRunner
import os
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import io
import contextlib

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
    output = sa.Column(sa.Text, nullable=True)

Base.metadata.create_all(bind=engine)

async def run_pipeline_task(run_id: int):
    db = SessionLocal()
    try:
        logger.info(f"Starting pipeline run {run_id}")

        output_buffer = io.StringIO()
        with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
            try:
                # Find project and configure
                project = Project.find()
                meltano.core.tracking.DISABLED = True

                logger.info("Project found, preparing to run pipeline")

                # Create and configure CLI context
                context = CliContext(command="elt")
                context.command_args = ["tap-csv", "target-postgres", "--job_id", str(run_id)]

                logger.info(f"Executing pipeline with args: {context.command_args}")

                # Import the proper command class
                from meltano.core.runner import CliRunner

                # Create the runner
                runner = CliRunner(project)

                # Execute the ELT command
                result = runner.invoke(["elt", "tap-csv", "target-postgres", "--job_id", str(run_id)])

                logger.info(f"Pipeline execution completed with exit code: {result.exit_code}")
                if result.exit_code != 0:
                    raise Exception(f"Pipeline failed with exit code {result.exit_code}")

            except Exception as e:
                logger.error(f"Error during pipeline execution: {str(e)}")
                raise

        # Get the captured output
        output_text = output_buffer.getvalue()
        logger.info(f"Captured output: {output_text[:200]}...")  # Log first 200 chars

        pipeline_run = db.query(PipelineRun).get(run_id)
        pipeline_run.status = "completed"
        pipeline_run.output = output_text
        logger.info(f"Pipeline run {run_id} completed successfully")

        db.commit()
    except Exception as e:
        logger.error(f"Pipeline run {run_id} failed: {str(e)}")
        pipeline_run = db.query(PipelineRun).get(run_id)
        pipeline_run.status = "failed"
        pipeline_run.output = str(e)
        db.commit()
        raise e
    finally:
        db.close()

@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Meltano Pipeline Control</title>
        <script>
            async function loadHistory() {
                try {
                    const response = await fetch('/runs');
                    const data = await response.json();
                    const statusDiv = document.getElementById('status');
                    statusDiv.innerHTML = '';

                    data.runs.forEach(run => {
                        addRunToHistory(run);
                    });
                } catch (error) {
                    console.error('Error loading history:', error);
                }
            }

            function addRunToHistory(run) {
                const statusDiv = document.getElementById('status');
                const newStatus = document.createElement('div');
                newStatus.className = `status-item ${run.status}`;
                newStatus.innerHTML = `
                    <div class="run-header" onclick="showDetails(${run.run_id})">
                        Run ${run.run_id}: ${run.status}
                        <br>
                        Started: ${new Date(run.start_time).toLocaleString()}
                    </div>
                `;
                statusDiv.insertBefore(newStatus, statusDiv.firstChild);
            }

            async function startPipeline() {
                const button = document.getElementById('runButton');
                button.disabled = true;
                try {
                    const response = await fetch('/run', { method: 'POST' });
                    const data = await response.json();
                    button.disabled = false;
                    updateStatus(data.run_id);
                } catch (error) {
                    console.error('Error:', error);
                    button.disabled = false;
                }
            }

            async function updateStatus(runId) {
                if (!runId) return;

                try {
                    const response = await fetch(`/status/${runId}`);
                    const data = await response.json();

                    const existingRun = document.querySelector(`.status-item[data-run-id="${data.run_id}"]`);
                    if (existingRun) {
                        existingRun.className = `status-item ${data.status}`;
                    } else {
                        addRunToHistory(data);
                    }

                    if (data.status === 'started') {
                        setTimeout(() => updateStatus(runId), 2000);
                    }
                } catch (error) {
                    console.error('Error:', error);
                }
            }

            async function showDetails(runId) {
                try {
                    const response = await fetch(`/status/${runId}`);
                    const data = await response.json();

                    const modal = document.getElementById('detailsModal');
                    const content = document.getElementById('modalContent');
                    modal.style.display = 'block';

                    content.innerHTML = `
                        <h3>Pipeline Run ${data.run_id}</h3>
                        <p><strong>Status:</strong> ${data.status}</p>
                        <p><strong>Started:</strong> ${new Date(data.start_time).toLocaleString()}</p>
                        ${data.output ? `<pre class="output">${data.output}</pre>` : ''}
                    `;
                } catch (error) {
                    console.error('Error loading details:', error);
                }
            }

            function closeModal() {
                const modal = document.getElementById('detailsModal');
                modal.style.display = 'none';
            }

            // Load history when page loads
            document.addEventListener('DOMContentLoaded', loadHistory);
        </script>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                margin: 2rem;
                line-height: 1.5;
            }
            .container { max-width: 800px; margin: 0 auto; }
            button {
                background: #4CAF50;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
            }
            button:disabled {
                background: #cccccc;
            }
            .status-item {
                margin: 10px 0;
                padding: 15px;
                border-radius: 4px;
                background: #f5f5f5;
                cursor: pointer;
                transition: background-color 0.2s;
            }
            .status-item:hover {
                background: #e0e0e0;
            }
            .started { border-left: 4px solid #2196F3; }
            .completed { border-left: 4px solid #4CAF50; }
            .failed { border-left: 4px solid #f44336; }
            h1 { color: #333; }
            .status-container {
                margin-top: 2rem;
                padding: 1rem;
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .modal {
                display: none;
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0,0,0,0.5);
            }
            .modal-content {
                background: white;
                margin: 10% auto;
                padding: 20px;
                width: 80%;
                max-width: 700px;
                border-radius: 8px;
                position: relative;
            }
            .close {
                position: absolute;
                right: 20px;
                top: 10px;
                font-size: 24px;
                cursor: pointer;
            }
            .output {
                background: #f8f9fa;
                padding: 15px;
                border-radius: 4px;
                overflow-x: auto;
                white-space: pre-wrap;
                font-family: monospace;
            }
            .run-header {
                cursor: pointer;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Meltano Pipeline Control</h1>
            <button id="runButton" onclick="startPipeline()">Start Pipeline</button>

            <div class="status-container">
                <h2>Pipeline Runs</h2>
                <div id="status"></div>
            </div>
        </div>

        <div id="detailsModal" class="modal">
            <div class="modal-content">
                <span class="close" onclick="closeModal()">&times;</span>
                <div id="modalContent"></div>
            </div>
        </div>
    </body>
    </html>
    """

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

@app.get("/runs")
async def get_runs():
    db = SessionLocal()
    try:
        runs = db.query(PipelineRun).order_by(PipelineRun.start_time.desc()).all()
        return {
            "runs": [{
                "run_id": run.id,
                "status": run.status,
                "start_time": run.start_time,
                "output": run.output
            } for run in runs]
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
            "start_time": pipeline_run.start_time,
            "output": pipeline_run.output
        }
    finally:
        db.close()
