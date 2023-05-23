from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI
from mela import Mela
from mela.settings import Settings
from pydantic import BaseModel

app = FastAPI()

mela_app = Mela(__name__)
mela_app.settings = Settings()


class ReportRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    user_id: str
    report_id: str | None = None


@app.post("/report")
async def read_root(report_request: ReportRequest):
    if report_request.report_id is None:
        report_request.report_id = str(uuid4())
    # some DB writing
    publisher = await mela_app.publisher_instance('report-generator')
    await publisher.publish(report_request)
    return report_request
