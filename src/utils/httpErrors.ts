import { Response } from 'express';

export const sendInternalError = (
  res: Response,
  clientMessage = 'Internal server error',
  code?: string
) => res.status(500).json({ error: clientMessage, ...(code ? { code } : {}) });

export const sendCodedError = (
  res: Response,
  status: number,
  code: string,
  clientMessage: string,
  field?: string
) => res.status(status).json({
  error: clientMessage,
  code,
  ...(field ? { field } : {})
});
