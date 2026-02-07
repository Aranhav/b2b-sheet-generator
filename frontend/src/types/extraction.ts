/** TypeScript types mirroring the backend Pydantic models. */

export interface ConfidenceValue {
  value: string | number | null;
  confidence: number;
}

export interface LineItem {
  description: ConfidenceValue;
  hs_code_origin: ConfidenceValue;
  hs_code_destination: ConfidenceValue;
  quantity: ConfidenceValue;
  unit_price_usd: ConfidenceValue;
  total_price_usd: ConfidenceValue;
  unit_weight_kg: ConfidenceValue;
  igst_percent: ConfidenceValue;
}

export interface BoxItem {
  description: ConfidenceValue;
  quantity: ConfidenceValue;
}

export interface Box {
  box_number: ConfidenceValue;
  length_cm: ConfidenceValue;
  width_cm: ConfidenceValue;
  height_cm: ConfidenceValue;
  gross_weight_kg: ConfidenceValue;
  net_weight_kg: ConfidenceValue;
  items: BoxItem[];
  destination_id: ConfidenceValue;
  receiver: Address | null;
}

export interface Address {
  name: ConfidenceValue;
  address: ConfidenceValue;
  city: ConfidenceValue;
  state: ConfidenceValue;
  zip_code: ConfidenceValue;
  country: ConfidenceValue;
  phone: ConfidenceValue;
  email: ConfidenceValue;
}

export interface InvoiceData {
  invoice_number: ConfidenceValue;
  invoice_date: ConfidenceValue;
  currency: ConfidenceValue;
  total_amount: ConfidenceValue;
  exporter: Address;
  consignee: Address;
  ship_to: Address;
  ior: Address;
  line_items: LineItem[];
}

export interface Destination {
  id: string;
  name: ConfidenceValue;
  address: Address;
}

export interface PackingListData {
  total_boxes: ConfidenceValue;
  total_net_weight_kg: ConfidenceValue;
  total_gross_weight_kg: ConfidenceValue;
  boxes: Box[];
  destinations: Destination[];
}

export interface ExtractionResult {
  job_id: string;
  status: string;
  overall_confidence: number;
  invoice: InvoiceData;
  packing_list: PackingListData;
  warnings: string[];
  errors: string[];
}

export interface JobStatus {
  job_id: string;
  status: string;
  progress: number;
  message: string;
  result: ExtractionResult | null;
  multi_address_download: string | null;
  simplified_download: string | null;
}

export interface ExtractionOptions {
  output_currency: 'auto' | 'USD' | 'INR';
  exchange_rate: number | null;
  sync_hs_codes: boolean;
}
