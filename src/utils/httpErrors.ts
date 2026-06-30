import { Response } from 'express';

export const sendInternalError = (
  res: Response,
  clientMessage = 'Internal server error'
) => res.status(500).json({ error: clientMessage });
