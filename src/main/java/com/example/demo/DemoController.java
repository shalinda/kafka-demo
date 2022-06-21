
// Java Program to Illustrate Controller Class

package com.example.demo;

// Importing required classes
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

// Annotation
@RestController
// Class
public class DemoController {

	// Autowiring Kafka Template
	@Autowired KafkaTemplate<String, String> kafkaTemplate;

	private static final String TOPIC = "NewTopic";

	// Publish messages using the GetMapping
	@GetMapping("/publish/{message}/{count}")
	public String publishMessage(@PathVariable("message") final String message,
								 @PathVariable("count") final int count)
	{
		String newJson = "{\n" +
				"\"tripPlanNumber\":1,\n" +
				"\"tripPlanVersion\":2,\n" +
				"\"carInitial\":\"AAAAAAAAAAAAAA\",\n" +
				"\"carNumber\":\"BBBBBBBBBBBBBBB\",\n" +
				"\"lastScheduleEventSeq\":3,\n" +
				"\"lastEventPostDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"waybillAuditNumber\":\"CCCCCCCCCCCCCC\",\n" +
				"\"waybillDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"waybillNumber\":\"DDDDDDDDDDDDDDD\",\n" +
				"\"waybillRoadCode\":\"EEEEEEEEEEEEEEE\",\n" +
				"\"ultimateOriginCity\":\"FFFFFFFFFFFFFFF\",\n" +
				"\"ultimateOriginState\":\"GGGGGGGGGGGGGGG\",\n" +
				"\"ultimateDestinationCity\":\"HHHHHHHHHHHHHHH\",\n" +
				"\"ultimateDestinationState\":\"IIIIIIIIIIIIIII\",\n" +
				"\"originalShipper\":\"JJJJJJJJJJJJJJJ\",\n" +
				"\"originalConsignee\":\"KKKKKKKKKKKKKK\",\n" +
				"\"requiredDeliveryDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"requiredDeliveryTimeMillis\":4,\n" +
				"\"tripRecordStatus\":\"LLLLLLLLLLLLLLL\",\n" +
				"\"priorTripRecordStatus\":\"MMMMMMMMMMMMMMM\",\n" +
				"\"trainID\":\"NNNNNNNNNNNNNNN\",\n" +
				"\"interceptDateTime\":\"2022-09-01 23:09:23.457\",\n" +
				"\"interceptRequestor\":\"OOOOOOOOOOOOOOO\",\n" +
				"\"unscheduledEvents\":5,\n" +
				"\"scheduledEvents\":6,\n" +
				"\"scheduleID\":7,\n" +
				"\"scheduleEffectiveDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"scheduleExpiryDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"scheduleOriginCity\":\"PPPPPPPPPPPPPPP\",\n" +
				"\"scheduleOriginState\":\"QQQQQQQQQQQQQQQ\",\n" +
				"\"scheduleDestinCity\":\"RRRRRRRRRRRRRRR\",\n" +
				"\"scheduleDestinState\":\"SSSSSSSSSSSSSSS\",\n" +
				"\"scheduleOriginRailroad\":\"TTTTTTTTTTTTTTT\",\n" +
				"\"scheduleETADateTime\":\"2022-09-01 23:09:23.457\",\n" +
				"\"scheduleETATimeMillis\":8,\n" +
				"\"estimatedETADateTime\":\"2022-09-01 23:09:23.457\",\n" +
				"\"estimatedETATimeMillis\":9,\n" +
				"\"eTATimeZone\":\"UUUUUUUUUUUUUUU\",\n" +
				"\"tripPlanScheduleMins\":10,\n" +
				"\"tripPlanActualMins\":11,\n" +
				"\"loadEmptyStatus\":\"VVVVVVVVVVVVVVV\",\n" +
				"\"aARCarType\":\"WWWWWWWWWWWWWWW\",\n" +
				"\"shipperID\":12,\n" +
				"\"consigneeID\":13,\n" +
				"\"commodityName\":\"XXXXXXXXXXXXXXX\",\n" +
				"\"comments\":\"YYYYYYYYYYYYYYY\",\n" +
				"\"bOLNumber\":\"ZZZZZZZZZZZZZZZ\",\n" +
				"\"bOLDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"sTCC\":\"AAAAAAAAAAAAAA\",\n" +
				"\"route1\":\"BBBBBBBBBBBBBBB\",\n" +
				"\"route2\":\"CCCCCCCCCCCCCC\",\n" +
				"\"route3\":\"DDDDDDDDDDDDDDD\",\n" +
				"\"junction1\":\"EEEEEEEEEEEEEEE\",\n" +
				"\"junction2\":\"FFFFFFFFFFFFFFF\",\n" +
				"\"junction3\":\"GGGGGGGGGGGGGGG\",\n" +
				"\"freightPayer\":\"HHHHHHHHHHHHHHH\",\n" +
				"\"currentEventCity\":\"IIIIIIIIIIIIIII\",\n" +
				"\"currentEventState\":\"JJJJJJJJJJJJJJJ\",\n" +
				"\"currentEventCode\":\"KKKKKKKKKKKKKK\",\n" +
				"\"currentEventType\":\"LLLLLLLLLLLLLLL\",\n" +
				"\"currentEventDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"receivedDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"currentCarrier\":\"MMMMMMMMMMMMMMM\",\n" +
				"\"currentStatus\":\"NNNNNNNNNNNNNNN\",\n" +
				"\"lastCommentDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"lastCommentOrganizationID\":15,\n" +
				"\"freightPayerID\":16,\n" +
				"\"beneficialOwner\":\"OOOOOOOOOOOOOOO\",\n" +
				"\"beneficialOwnerID\":17,\n" +
				"\"careOfParty\":\"PPPPPPPPPPPPPPP\",\n" +
				"\"careOfPartyID\":18,\n" +
				"\"notifyParty\":\"QQQQQQQQQQQQQQQ\",\n" +
				"\"notifyPartyID\":19,\n" +
				"\"propertyOwner\":\"RRRRRRRRRRRRRRR\",\n" +
				"\"propertyOwnerID\":20,\n" +
				"\"equipmentGrossWeight\":\"SSSSSSSSSSSSSSS\",\n" +
				"\"equipmentTareWeight\":\"TTTTTTTTTTTTTTT\",\n" +
				"\"equipmentActualNetWeight\":\"UUUUUUUUUUUUUUU\",\n" +
				"\"equipmentEstNetWeight\":\"VVVVVVVVVVVVVVV\",\n" +
				"\"weightUnit\":\"WWWWWWWWWWWWWWW\",\n" +
				"\"nonServiceDays\":21,\n" +
				"\"nonServiceDaysType\":\"XXXXXXXXXXXXXXX\",\n" +
				"\"unloadingTime\":22,\n" +
				"\"orderNumber\":\"YYYYYYYYYYYYYYY\",\n" +
				"\"fleetCode\":\"ZZZZZZZZZZZZZZZ\",\n" +
				"\"subFleetCode\":\"AAAAAAAAAAAAAA\",\n" +
				"\"fleetCustomer\":\"BBBBBBBBBBBBBBB\",\n" +
				"\"fleetCommodity\":\"CCCCCCCCCCCCCC\",\n" +
				"\"dowGMID\":\"DDDDDDDDDDDDDDD\",\n" +
				"\"dowGMIDDescription\":\"EEEEEEEEEEEEEEE\",\n" +
				"\"gallons\":\"FFFFFFFFFFFFFFF\",\n" +
				"\"accountOfParty\":\"GGGGGGGGGGGGGGG\",\n" +
				"\"accountOfPartyID\":23,\n" +
				"\"currentLoadStatus\":\"HHHHHHHHHHHHHHH\",\n" +
				"\"currentDestination\":\"IIIIIIIIIIIIIII\",\n" +
				"\"customsBroker\":\"JJJJJJJJJJJJJJJ\",\n" +
				"\"shipmentCategory1\":\"KKKKKKKKKKKKKK\",\n" +
				"\"shipmentCategory2\":\"LLLLLLLLLLLLLLL\",\n" +
				"\"deleteUserName\":\"MMMMMMMMMMMMMMM\",\n" +
				"\"deleteDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"customsBrokerID\":25,\n" +
				"\"purchaseOrderNumber\":\"NNNNNNNNNNNNNNN\",\n" +
				"\"cLMTrainID\":\"OOOOOOOOOOOOOOO\",\n" +
				"\"cLMEventDate\":\"2022-09-01 23:09:23.457\",\n" +
				"\"cLMRailRoad\":\"PPPPPPPPPPPPPPP\",\n" +
				"\"salesDepartment\":\"QQQQQQQQQQQQQQQ\",\n" +
				"\"productGroup\":\"RRRRRRRRRRRRRRR\",\n" +
				"\"product\":\"SSSSSSSSSSSSSSS\",\n" +
				"\"profitCenter\":\"TTTTTTTTTTTTTTT\",\n" +
				"\"costCenter\":\"UUUUUUUUUUUUUUU\",\n" +
				"\"customsBroker2\":\"VVVVVVVVVVVVVVV\",\n" +
				"\"customsBroker2ID\":26,\n" +
				"\"forwarder\":\"WWWWWWWWWWWWWWW\",\n" +
				"\"forwarderID\":27,\n" +
				"\"dowOrderCompanyCode\":\"XXXXXXXXXXXXXXX\",\n" +
				"\"dowOrderNumber\":\"YYYYYYYYYYYYYYY\",\n" +
				"\"shipmentCompanyCode\":\"ZZZZZZZZZZZZZZZ\",\n" +
				"\"grossGallons\":\"AAAAAAAAAAAAAA\",\n" +
				"\"freightContractNumber\":\"BBBBBBBBBBBBBBB\",\n" +
				"\"consigneeOrderNumber\":\"CCCCCCCCCCCCCC\",\n" +
				"\"customerReferenceNumber\":\"DDDDDDDDDDDDDDD\",\n" +
				"\"billingMethod\":\"EEEEEEEEEEEEEEE\",\n" +
				"\"commodityGrade\":\"FFFFFFFFFFFFFFF\",\n" +
				"\"route4\":\"GGGGGGGGGGGGGGG\",\n" +
				"\"junction4\":\"HHHHHHHHHHHHHHH\",\n" +
				"\"route5\":\"IIIIIIIIIIIIIII\",\n" +
				"\"junction5\":\"JJJJJJJJJJJJJJJ\",\n" +
				"\"route6\":\"KKKKKKKKKKKKKK\",\n" +
				"\"junction6\":\"LLLLLLLLLLLLLLL\",\n" +
				"\"route7\":\"MMMMMMMMMMMMMMM\",\n" +
				"\"junction7\":\"NNNNNNNNNNNNNNN\",\n" +
				"\"route8\":\"OOOOOOOOOOOOOOO\",\n" +
				"\"junction8\":\"PPPPPPPPPPPPPPP\",\n" +
				"\"route9\":\"QQQQQQQQQQQQQQQ\",\n" +
				"\"junction9\":\"RRRRRRRRRRRRRRR\",\n" +
				"\"route10\":\"SSSSSSSSSSSSSSS\",\n" +
				"\"junction10\":\"TTTTTTTTTTTTTTT\",\n" +
				"\"rule11Payer\":\"UUUUUUUUUUUUUUU\",\n" +
				"\"rule11PayerID\":28,\n" +
				"\"lessorOrganization\":\"VVVVVVVVVVVVVVV\",\n" +
				"\"lessorOrganizationID\":29,\n" +
				"\"isChange\":\"WWWWWWWWWWWWWWW\",\n" +
				"\"commodityDescription\":\"XXXXXXXXXXXXXXX\",\n" +
				"\"manualETAFlag\":\"YYYYYYYYYYYYYYY\" \n" +
				"}";
		long time1 =System.nanoTime();
		for (int i = 0; i < count; i++) {
			// Sending the message
			kafkaTemplate.send(TOPIC, newJson + "-" + i);
		}
		System.out.println("time ms: " + (System.nanoTime() - time1)/1000000);

		return "Published " + count + " messages Successfully";
	}
}