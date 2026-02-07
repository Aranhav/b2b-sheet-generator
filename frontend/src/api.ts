import axios from 'axios';
import type { JobStatus, ExtractionOptions } from './types/extraction';

// In dev, Vite proxy forwards /api to localhost:8000
// In prod, same origin serves both frontend and API
const API_BASE = '';

const api = axios.create({
  baseURL: API_BASE,
});

export const extractDocuments = async (
  files: File[],
  options?: ExtractionOptions,
): Promise<JobStatus> => {
  const formData = new FormData();
  files.forEach((file) => formData.append('files', file));

  if (options) {
    formData.append('output_currency', options.output_currency);
    if (options.exchange_rate !== null) {
      formData.append('exchange_rate', String(options.exchange_rate));
    }
    formData.append('sync_hs_codes', String(options.sync_hs_codes));
  }

  const response = await api.post<JobStatus>('/api/extract', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: 180000,
  });
  return response.data;
};

export const getJobStatus = async (jobId: string): Promise<JobStatus> => {
  const response = await api.get<JobStatus>(`/api/jobs/${jobId}`);
  return response.data;
};

export const getDownloadUrl = (jobId: string, type: 'multi' | 'simplified' | 'b2b_shipment' | 'result') =>
  `/api/download/${jobId}/${type}`;

export default api;
