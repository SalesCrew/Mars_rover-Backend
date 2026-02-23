import { Router } from 'express';

const router = Router();

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
    console.error('Error in /driving-times:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
