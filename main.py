from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import HTMLResponse
import meltano.core.tracking
from meltano.core.project import Project
import os
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging
import subprocess
import re

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

def clean_ansi(text):
    """Remove ANSI color codes from text."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def get_data_summary(db):
    """Get summary statistics from loaded data."""
    try:
        # Use fixed table name
        table_name = "sales_data"  # Matches the entity name in tap-csv config

        query = text(f"""
            SELECT
                COUNT(*) as total_records,
                SUM(revenue) as total_revenue,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM "{table_name}";
        """)
        result = db.execute(query).fetchone()

        if not result or not result.total_records:
            return "No records found in the data table"

        top_query = text(f"""
            SELECT
                name,
                revenue,
                date,
                RANK() OVER (ORDER BY revenue DESC) as revenue_rank
            FROM "{table_name}"
            ORDER BY revenue DESC
            LIMIT 3;
        """)
        top_products = db.execute(top_query).fetchall()

        summary = f"""Data Summary:
-------------
Total Records: {result.total_records}
Total Revenue: ${result.total_revenue:,.2f}
Date Range: {result.earliest_date} to {result.latest_date}

Top Products by Revenue:
---------------------""" + "\n".join(f"\n{row.name}: ${row.revenue:,.2f} ({row.date})"
                for row in top_products)

        return summary
    except Exception as e:
        logger.error(f"Error getting data summary: {str(e)}")
        return f"Error analyzing data: {str(e)}"

async def run_pipeline_task(run_id: int):
    db = SessionLocal()
    try:
        logger.info(f"Starting pipeline run {run_id}")
        pipeline_run = db.query(PipelineRun).get(run_id)

        try:
            project = Project.find()
            project_dir = project.root

            # Execute pipeline
            process = subprocess.Popen(
                ['meltano', 'el', 'tap-csv', 'target-postgres', '--full-refresh'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=project_dir,
                env=os.environ.copy(),
                text=True
            )

            stdout, stderr = process.communicate()
            clean_output = clean_ansi(stderr)

            if process.returncode != 0:
                error_msg = f"Pipeline failed with return code {process.returncode}"
                pipeline_run.status = "failed"
                pipeline_run.output = f"Error:\n{clean_output}"
                db.commit()
                raise Exception(error_msg)

            # Get data summary
            data_summary = get_data_summary(db)

            pipeline_run.status = "completed"
            pipeline_run.output = f"""Pipeline Execution Summary:
-------------------------
Status: Completed Successfully
Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}

{data_summary}

Execution Log:
------------
{clean_output}"""

            db.commit()
            logger.info("Pipeline run completed successfully")

        except Exception as e:
            error_msg = f"Error during pipeline execution: {str(e)}"
            logger.error(error_msg)
            pipeline_run.status = "failed"
            pipeline_run.output = error_msg
            db.commit()
            raise

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
                newStatus.setAttribute('data-run-id', run.run_id);

                const timestamp = new Date(run.start_time).toLocaleString();
                newStatus.innerHTML = `
                    <div class="run-summary">
                        <div class="run-info">
                            <div class="run-title">Run ${run.run_id}</div>
                            <div class="run-details">
                                <span class="status-badge ${run.status}">${run.status}</span>
                                <span class="timestamp">${timestamp}</span>
                            </div>
                        </div>
                        <a href="#/run/${run.run_id}" class="view-details" onclick="showDetails(${run.run_id}); return false;">
                            View Details
                        </a>
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

                    const detailsPage = document.getElementById('run-details');
                    if (detailsPage && detailsPage.getAttribute('data-run-id') == runId) {
                        updateDetailsPage(data);
                    }
                } catch (error) {
                    console.error('Error:', error);
                }
            }

            async function showDetails(runId) {
                try {
                    const response = await fetch(`/status/${runId}`);
                    const data = await response.json();

                    const mainContent = document.getElementById('main-content');
                    const detailsPage = document.getElementById('run-details');

                    mainContent.style.display = 'none';
                    detailsPage.style.display = 'block';
                    detailsPage.setAttribute('data-run-id', runId);

                    updateDetailsPage(data);
                    window.history.pushState({}, '', `#/run/${runId}`);
                } catch (error) {
                    console.error('Error loading details:', error);
                }
            }

            function updateDetailsPage(data) {
                const detailsPage = document.getElementById('run-details');
                const timestamp = new Date(data.start_time).toLocaleString();

                detailsPage.innerHTML = `
                    <div class="details-header">
                        <button onclick="showMain()" class="back-button">← Back to List</button>
                        <h2>Pipeline Run ${data.run_id}</h2>
                    </div>
                    <div class="details-content">
                        <div class="details-summary">
                            <div class="summary-item">
                                <label>Status:</label>
                                <span class="status-badge ${data.status}">${data.status}</span>
                            </div>
                            <div class="summary-item">
                                <label>Started:</label>
                                <span>${timestamp}</span>
                            </div>
                        </div>
                        <div class="output-section">
                            <pre class="output">${data.output || 'No output available'}</pre>
                        </div>
                    </div>
                `;
            }

            function showMain() {
                const mainContent = document.getElementById('main-content');
                const detailsPage = document.getElementById('run-details');

                mainContent.style.display = 'block';
                detailsPage.style.display = 'none';
                window.history.pushState({}, '', '/');
            }

            window.onpopstate = function(event) {
                const hash = window.location.hash;
                if (hash.startsWith('#/run/')) {
                    const runId = hash.split('/')[2];
                    showDetails(runId);
                } else {
                    showMain();
                }
            };

            document.addEventListener('DOMContentLoaded', () => {
                loadHistory();

                const hash = window.location.hash;
                if (hash.startsWith('#/run/')) {
                    const runId = hash.split('/')[2];
                    showDetails(runId);
                }
            });
        </script>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                margin: 0;
                line-height: 1.5;
                color: #333;
                background: #f5f5f5;
            }

            .container {
                max-width: 1000px;
                margin: 0 auto;
                padding: 2rem;
            }

            .page-header {
                background: white;
                padding: 1.5rem;
                margin-bottom: 2rem;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }

            button {
                background: #4CAF50;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
                transition: background-color 0.2s;
            }

            button:hover {
                background: #45a049;
            }

            button:disabled {
                background: #cccccc;
            }

            .back-button {
                background: #666;
                margin-right: 1rem;
            }

            .back-button:hover {
                background: #555;
            }

            .status-item {
                background: white;
                margin: 10px 0;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }

            .run-summary {
                padding: 1.5rem;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }

            .run-info {
                flex-grow: 1;
            }

            .run-title {
                font-size: 1.2rem;
                font-weight: 600;
                margin-bottom: 0.5rem;
            }

            .run-details {
                display: flex;
                gap: 1rem;
                align-items: center;
            }

            .status-badge {
                padding: 0.25rem 0.75rem;
                border-radius: 12px;
                font-size: 0.9rem;
                font-weight: 500;
            }

            .status-badge.started { background: #e3f2fd; color: #1565c0; }
            .status-badge.completed { background: #e8f5e9; color: #2e7d32; }
            .status-badge.failed { background: #ffebee; color: #c62828; }

            .timestamp {
                color: #666;
                font-size: 0.9rem;
            }

            .view-details {
                color: #2196F3;
                text-decoration: none;
                font-weight: 500;
            }

            .view-details:hover {
                text-decoration: underline;
            }

            .details-header {
                display: flex;
                align-items: center;
                margin-bottom: 2rem;
                padding-bottom: 1rem;
                border-bottom: 1px solid #eee;
            }

            .details-content {
background: white;
                padding: 2rem;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }

            .details-summary {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 1rem;
                margin-bottom: 2rem;
            }

            .summary-item label {
                display: block;
                font-weight: 500;
                margin-bottom: 0.5rem;
                color: #666;
            }

            .output-section {
                background: #f8f9fa;
                padding: 1.5rem;
                border-radius: 4px;
            }

            .output {
                background: #fff;
                padding: 1rem;
                border-radius: 4px;
                overflow-x: auto;
                white-space: pre-wrap;
                font-family: "SF Mono", Consolas, "Liberation Mono", Menlo, monospace;
                font-size: 0.9rem;
                line-height: 1.5;
                color: #333;
                border: 1px solid #eee;
            }

            #run-details {
                display: none;
            }

            h1, h2 {
                color: #333;
                margin: 0;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div id="main-content">
                <div class="page-header">
                    <h1>Meltano Pipeline Control</h1>
                </div>

                <button id="runButton" onclick="startPipeline()">Start Pipeline</button>

                <div class="status-container">
                    <h2>Pipeline Runs</h2>
                    <div id="status"></div>
                </div>
            </div>

            <div id="run-details"></div>
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
