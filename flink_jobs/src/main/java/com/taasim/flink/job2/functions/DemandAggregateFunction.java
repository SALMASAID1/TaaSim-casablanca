package com.taasim.flink.job2.functions;

import com.taasim.flink.job2.model.DemandZoneAggregate;
import com.taasim.flink.job2.model.UnifiedWindowInput;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DemandAggregateFunction extends ProcessWindowFunction<UnifiedWindowInput, DemandZoneAggregate, Integer, TimeWindow> {

    @Override
    public void process(
            Integer zoneId,
            Context context,
            Iterable<UnifiedWindowInput> elements,
            Collector<DemandZoneAggregate> out) {

        final Set<String> activeVehicles = new HashSet<>();
        int pendingRequests = 0;
        String city = "casablanca";

        for (UnifiedWindowInput element : elements) {
            if (element.city != null && !element.city.isBlank()) {
                city = element.city;
            }
            if ("VEHICLE".equalsIgnoreCase(element.eventType)) {
                if (element.entityId != null && !element.entityId.isBlank()) {
                    activeVehicles.add(element.entityId);
                }
            } else if ("REQUEST".equalsIgnoreCase(element.eventType)) {
                pendingRequests++;
            }
        }

        final int activeVehiclesCount = activeVehicles.size();
        final float ratio = (float) pendingRequests / Math.max(activeVehiclesCount, 1);

        final DemandZoneAggregate aggregate = new DemandZoneAggregate();
        aggregate.city = city;
        aggregate.zoneId = zoneId;
        aggregate.windowStart = context.window().getStart();
        aggregate.activeVehicles = activeVehiclesCount;
        aggregate.pendingRequests = pendingRequests;
        aggregate.ratio = ratio;
        aggregate.forecastDemand = 0.0f;

        out.collect(aggregate);
    }
}
