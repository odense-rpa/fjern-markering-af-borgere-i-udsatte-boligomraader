import asyncio
import logging
import sys

from odk_tools.tracking import Tracker
from momentum_client.manager import MomentumClientManager
from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus

tracker: Tracker
momentum: MomentumClientManager
proces_navn = "Fjern markering af borgere i udsatte boligområder"
MARKERINGERSNAVNE = [ "JP Vollsmose gruppe 1", "JP Vollsmose gruppe 2", "JP Vollsmose gruppe 3",
                     "JP KSE gruppe 1", "JP KSE gruppe 2", "JP KSE gruppe 3"  ]
async def populate_queue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)

    logger.info("Hello from populate workqueue!")

    # sætter filtre op til at finde alle andre end 6.1 & 6.2 borgere, der som har en af de 6 markeringer
    filters = [
        {
            "customFilter": "exclude",
            "fieldName": "targetGroupCode",
            "values": [
                "6.1",
                "6.2",
            ]
        },
        {
            "customFilter": "active",
            "fieldName" : "tags/id",
            "values" : [
                None,
                None,
                None,
                None,
                "20456975-a836-4055-a12f-ab06cc5c5ee4",
                "d00ec477-aa7b-4ee1-b12f-ca4c91fe4b58",
                "80121ed2-4cd8-4d5f-846f-81a098f0d95b",
                "b471a3ed-09f5-411e-9b11-0b1d0d9aaa86",
                "162f3394-92d1-45a1-b0a7-18c4b0c5f530",
                "6564b48b-e04f-42b5-a8a7-5926efd759ed"

            ]
        }
    ]   
    borgere = momentum.borgere.hent_borgere(filters=filters)
    
    for borger in borgere["data"]:
        # Finder markering, der skal afsluttes.
        borgers_markeringer = borger["tags"]
        markering = next((markering for markering in borgers_markeringer if markering["title"] in MARKERINGERSNAVNE and markering["end"] == None), None)
        
        data = {
            "cpr": borger["cpr"],
            "markering": markering["title"],
        }
        workqueue.add_item(data=data, reference=borger["cpr"])




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
