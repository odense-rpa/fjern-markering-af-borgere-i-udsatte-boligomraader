import asyncio
import logging
import sys

from odk_tools.tracking import Tracker
from momentum_client.manager import MomentumClientManager
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus

tracker: Tracker
momentum: MomentumClientManager
proces_navn = "Fjern markering af borgere i udsatte boligområder"

async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from process workqueue!")

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
 
            try:
                # Process the item here
                pass
            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    # Initialize external systems for automation here..
        # Initialize external systems for automation here..
    tracking_credential = Credential.get_credential("Odense SQL Server")
    momentum_credential = Credential.get_credential("Momentum - produktion")
    # momentum_credential = Credential.get_credential("Momentum - edu")

    tracker = Tracker(
        username=tracking_credential.username,
        password=tracking_credential.password
    )

    momentum = MomentumClientManager(
        base_url=momentum_credential.data["base_url"],
        client_id=momentum_credential.username,
        client_secret=momentum_credential.password,
        api_key=momentum_credential.data["api_key"],
        resource=momentum_credential.data["resource"],
    )

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
