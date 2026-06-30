import { Router } from 'express';

const router = Router();

const MAX_DRIVING_TIME_PAIRS = 25;
const MAX_ROUTE_STOPS = 25;
const MAX_ADDRESS_LENGTH = 300;

const isValidAddress = (value: unknown): value is string =>
  typeof value === 'string' &&
  value.trim().length > 0 &&
  value.trim().length <= MAX_ADDRESS_LENGTH;

interface DrivingTimePair {
  originAddress: string;
  destinationAddress: string;
}

interface DrivingTimeResult {
  originAddress: string;
  destinationAddress: string;
  durationSeconds: number;
  durationText: string;
  distanceMeters: number;
  distanceText: string;
  status: string;
}

router.post('/driving-times', async (req, res) => {
  try {
    const { pairs } = req.body as { pairs: DrivingTimePair[] };

    if (!pairs || !Array.isArray(pairs) || pairs.length === 0) {
      return res.status(400).json({ error: 'pairs array is required' });
    }

    if (pairs.length > MAX_DRIVING_TIME_PAIRS) {
      return res.status(400).json({ error: `pairs array must contain at most ${MAX_DRIVING_TIME_PAIRS} entries` });
    }

    if (!pairs.every(pair => isValidAddress(pair.originAddress) && isValidAddress(pair.destinationAddress))) {
      return res.status(400).json({ error: 'Each pair must include valid originAddress and destinationAddress values' });
    }

    const apiKey = process.env.GOOGLE_MAPS_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'Google Maps API key not configured' });
    }

    const results: DrivingTimeResult[] = [];

    for (const pair of pairs) {
      try {
        const origin = encodeURIComponent(pair.originAddress);
        const destination = encodeURIComponent(pair.destinationAddress);
        const url = `https://maps.googleapis.com/maps/api/distancematrix/json?origins=${origin}&destinations=${destination}&mode=driving&language=de&key=${apiKey}`;

        const response = await fetch(url);
        const data = await response.json() as any;

        if (data.status === 'OK' && data.rows?.[0]?.elements?.[0]?.status === 'OK') {
          const element = data.rows[0].elements[0];
          results.push({
            originAddress: pair.originAddress,
            destinationAddress: pair.destinationAddress,
            durationSeconds: element.duration.value,
            durationText: element.duration.text,
            distanceMeters: element.distance.value,
            distanceText: element.distance.text,
            status: 'OK',
          });
        } else {
          results.push({
            originAddress: pair.originAddress,
            destinationAddress: pair.destinationAddress,
            durationSeconds: 0,
            durationText: '-',
            distanceMeters: 0,
            distanceText: '-',
            status: data.rows?.[0]?.elements?.[0]?.status || data.status || 'UNKNOWN_ERROR',
          });
        }
      } catch (err) {
        results.push({
          originAddress: pair.originAddress,
          destinationAddress: pair.destinationAddress,
          durationSeconds: 0,
          durationText: '-',
          distanceMeters: 0,
          distanceText: '-',
          status: 'FETCH_ERROR',
        });
      }
    }

    res.json({ results });
  } catch (error) {
    console.error('Error in /driving-times:');
    res.status(500).json({ error: 'Internal server error' });
  }
});

interface OptimizeRouteStop {
  id: string;
  address: string;
}

router.post('/optimize-route', async (req, res) => {
  try {
    const { homeAddress, stops, mode = 'driving', optimize = true } = req.body as {
      homeAddress: string;
      stops: OptimizeRouteStop[];
      mode?: 'driving' | 'transit';
      optimize?: boolean;
    };

    if (!homeAddress || !stops || !Array.isArray(stops) || stops.length === 0) {
      return res.status(400).json({ error: 'homeAddress and stops array are required' });
    }

    if (!isValidAddress(homeAddress)) {
      return res.status(400).json({ error: 'homeAddress must be a non-empty address string' });
    }

    if (stops.length > MAX_ROUTE_STOPS) {
      return res.status(400).json({ error: `stops array must contain at most ${MAX_ROUTE_STOPS} entries` });
    }

    if (!['driving', 'transit'].includes(mode)) {
      return res.status(400).json({ error: 'mode must be driving or transit' });
    }

    if (!stops.every(stop => typeof stop.id === 'string' && stop.id.trim() && isValidAddress(stop.address))) {
      return res.status(400).json({ error: 'Each stop must include a valid id and address' });
    }

    const apiKey = process.env.GOOGLE_MAPS_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'Google Maps API key not configured' });
    }

    const origin = encodeURIComponent(homeAddress);
    const destination = encodeURIComponent(homeAddress);

    const waypointPrefix = optimize ? 'optimize:true|' : '';
    const waypointAddresses = stops.map(s => encodeURIComponent(s.address)).join('|');
    const waypoints = `${waypointPrefix}${waypointAddresses}`;

    const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${origin}&destination=${destination}&waypoints=${waypoints}&mode=${mode}&language=de&key=${apiKey}`;

    const response = await fetch(url);
    const data = await response.json() as any;

    if (data.status !== 'OK' || !data.routes?.[0]) {
      return res.status(400).json({
        error: 'Route calculation failed',
        googleStatus: data.status,
        message: data.error_message || 'No route found',
      });
    }

    const route = data.routes[0];
    const waypointOrder: number[] = route.waypoint_order || stops.map((_: any, i: number) => i);

    const optimizedOrder = waypointOrder.map((idx: number) => stops[idx].id);

    const legs = route.legs.map((leg: any) => ({
      durationSeconds: leg.duration?.value || 0,
      durationText: leg.duration?.text || '-',
      distanceText: leg.distance?.text || '-',
    }));

    let totalDrivingSeconds = 0;
    for (const leg of legs) {
      totalDrivingSeconds += leg.durationSeconds;
    }

    res.json({
      optimizedOrder,
      legs,
      totalDrivingSeconds,
    });
  } catch (error) {
    console.error('Error in /optimize-route:');
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
