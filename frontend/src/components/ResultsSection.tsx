import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  Card,
  CardContent,
  Grid,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  Alert,
  AlertTitle,
  Divider,
} from '@mui/material';
import {
  FileDownloadOutlined,
  DataObject,
  WarningAmberOutlined,
  RestartAltOutlined,
} from '@mui/icons-material';
import type { ExtractionResult, ConfidenceValue, Address, Destination } from '../types/extraction';

interface Props {
  result: ExtractionResult;
  jobId: string;
  onDownload: (type: 'multi' | 'simplified' | 'b2b_shipment' | 'result') => void;
  onReset: () => void;
}

const ResultsSection: React.FC<Props> = ({ result, jobId: _jobId, onDownload, onReset }) => {
  void _jobId;

  const getConfidenceColor = (score: number): 'success' | 'warning' | 'error' => {
    if (score >= 0.9) return 'success';
    if (score >= 0.7) return 'warning';
    return 'error';
  };

  const fmtCurrency = (value: number | string | null): string => {
    if (value === null || value === undefined || value === '') return '-';
    const num = typeof value === 'string' ? parseFloat(value) : value;
    if (isNaN(num)) return '-';
    return `$${num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const fmtWeight = (value: number | string | null): string => {
    if (value === null || value === undefined || value === '') return '-';
    const num = typeof value === 'string' ? parseFloat(value) : value;
    if (isNaN(num)) return '-';
    return `${num.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} kg`;
  };

  const v = (cv: ConfidenceValue): string => {
    if (cv.value === null || cv.value === undefined || cv.value === '') return '-';
    return String(cv.value);
  };

  const getReceiverAddress = (): Address => {
    const shipTo = result.invoice.ship_to;
    const hasData = shipTo.name.value || shipTo.address.value || shipTo.city.value || shipTo.country.value;
    return hasData ? shipTo : result.invoice.consignee;
  };

  const receiver = getReceiverAddress();
  const { invoice, packing_list } = result;
  const destinations: Destination[] = packing_list.destinations || [];
  const hasMultiDest = destinations.length > 1;

  // ---------------------------------------------------------------------------
  // Summary stats
  // ---------------------------------------------------------------------------
  const stats = [
    { label: 'Invoice', value: v(invoice.invoice_number), sub: v(invoice.invoice_date) },
    { label: 'Amount', value: fmtCurrency(invoice.total_amount.value) },
    { label: 'Items', value: String(invoice.line_items.length) },
    { label: 'Boxes', value: v(packing_list.total_boxes) },
    { label: 'Weight', value: fmtWeight(packing_list.total_gross_weight_kg.value) },
  ];

  if (hasMultiDest) {
    stats.push({ label: 'Destinations', value: String(destinations.length) });
  }

  // ---------------------------------------------------------------------------
  // Table styles
  // ---------------------------------------------------------------------------
  const thSx = {
    fontWeight: 600,
    color: '#475569',
    fontSize: '0.75rem',
    textTransform: 'uppercase' as const,
    letterSpacing: '0.03em',
    whiteSpace: 'nowrap' as const,
    py: 1.25,
    borderBottom: '2px solid #e8ecf1',
  };

  const tdSx = {
    fontSize: '0.83rem',
    color: '#334155',
    py: 1.25,
    borderBottom: '1px solid #f1f5f9',
  };

  const rowHover = {
    '&:hover': { backgroundColor: '#fafbfc' },
  };

  // ---------------------------------------------------------------------------
  // Download templates config
  // ---------------------------------------------------------------------------
  const downloadTemplates = [
    {
      key: 'multi' as const,
      name: 'Inline Template',
      desc: 'Flat format, one row per item-in-box with receiver inline',
      color: '#1a1a2e',
    },
    {
      key: 'b2b_shipment' as const,
      name: 'Split Template',
      desc: 'Grouped by destination, address header per shipment block',
      color: '#3b82f6',
    },
    {
      key: 'simplified' as const,
      name: 'Existing Template',
      desc: 'Multi-sheet format: Items, Receivers, Boxes, Standard Addresses',
      color: '#059669',
    },
  ];

  // ---------------------------------------------------------------------------
  // Receiver fields
  // ---------------------------------------------------------------------------
  const receiverFields = [
    { label: 'Name', value: v(receiver.name) },
    { label: 'Address', value: v(receiver.address) },
    { label: 'City', value: v(receiver.city) },
    { label: 'State', value: v(receiver.state) },
    { label: 'ZIP', value: v(receiver.zip_code) },
    { label: 'Country', value: v(receiver.country) },
    { label: 'Phone', value: v(receiver.phone) },
    { label: 'Email', value: v(receiver.email) },
  ];

  return (
    <Box sx={{ width: '100%' }}>
      {/* ------------------------------------------------------------------ */}
      {/* Header with confidence badge                                       */}
      {/* ------------------------------------------------------------------ */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 3,
        }}
      >
        <Box>
          <Typography variant="h5" sx={{ fontSize: '1.35rem', color: '#1a1a2e' }}>
            Extraction Results
          </Typography>
          <Typography sx={{ color: '#94a3b8', fontSize: '0.85rem', mt: 0.25 }}>
            Review extracted data and download formatted sheets
          </Typography>
        </Box>
        <Chip
          label={`${(result.overall_confidence * 100).toFixed(0)}% confidence`}
          color={getConfidenceColor(result.overall_confidence)}
          size="small"
          variant="filled"
          sx={{ fontWeight: 600 }}
        />
      </Box>

      {/* ------------------------------------------------------------------ */}
      {/* Summary stats row                                                  */}
      {/* ------------------------------------------------------------------ */}
      <Box
        sx={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 1.5,
          mb: 3,
        }}
      >
        {stats.map((s) => (
          <Paper
            key={s.label}
            sx={{
              flex: '1 1 0',
              minWidth: 120,
              px: 2,
              py: 1.5,
              borderRadius: '10px',
            }}
          >
            <Typography
              sx={{ color: '#94a3b8', fontSize: '0.7rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.25 }}
            >
              {s.label}
            </Typography>
            <Typography sx={{ fontWeight: 700, color: '#1a1a2e', fontSize: '1rem' }}>
              {s.value}
            </Typography>
            {'sub' in s && s.sub && (
              <Typography sx={{ color: '#94a3b8', fontSize: '0.75rem' }}>
                {s.sub}
              </Typography>
            )}
          </Paper>
        ))}
      </Box>

      {/* ------------------------------------------------------------------ */}
      {/* Destinations / Receiver                                            */}
      {/* ------------------------------------------------------------------ */}
      {hasMultiDest ? (
        <Card variant="outlined" sx={{ mb: 3, borderRadius: '10px' }}>
          <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
            <Box sx={{ px: 2.5, py: 1.5, borderBottom: '1px solid #f1f5f9' }}>
              <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.9rem' }}>
                Destinations ({destinations.length})
              </Typography>
            </Box>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell sx={thSx}>ID</TableCell>
                    <TableCell sx={thSx}>Name</TableCell>
                    <TableCell sx={thSx}>Address</TableCell>
                    <TableCell sx={thSx}>City</TableCell>
                    <TableCell sx={thSx}>State</TableCell>
                    <TableCell sx={thSx}>ZIP</TableCell>
                    <TableCell sx={thSx}>Country</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {destinations.map((dest, idx) => (
                    <TableRow key={dest.id || idx} sx={rowHover}>
                      <TableCell sx={tdSx}>
                        <Chip label={dest.id} size="small" sx={{ fontSize: '0.75rem', height: 22, backgroundColor: '#f1f5f9', color: '#475569', fontWeight: 600 }} />
                      </TableCell>
                      <TableCell sx={{ ...tdSx, fontWeight: 500 }}>{v(dest.name)}</TableCell>
                      <TableCell sx={tdSx}>{v(dest.address.address)}</TableCell>
                      <TableCell sx={tdSx}>{v(dest.address.city)}</TableCell>
                      <TableCell sx={tdSx}>{v(dest.address.state)}</TableCell>
                      <TableCell sx={tdSx}>{v(dest.address.zip_code)}</TableCell>
                      <TableCell sx={tdSx}>{v(dest.address.country)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </CardContent>
        </Card>
      ) : (
        <Card variant="outlined" sx={{ mb: 3, borderRadius: '10px' }}>
          <CardContent sx={{ px: 2.5, py: 2 }}>
            <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.9rem', mb: 1.5 }}>
              Ship To / Receiver
            </Typography>
            <Grid container spacing={1.5}>
              {receiverFields.map((f) => (
                <Grid size={{ xs: 6, sm: 3 }} key={f.label}>
                  <Typography sx={{ color: '#94a3b8', fontSize: '0.7rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                    {f.label}
                  </Typography>
                  <Typography sx={{ color: '#334155', fontSize: '0.83rem', fontWeight: 500 }}>
                    {f.value}
                  </Typography>
                </Grid>
              ))}
            </Grid>
          </CardContent>
        </Card>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Items table                                                        */}
      {/* ------------------------------------------------------------------ */}
      <Card variant="outlined" sx={{ mb: 3, borderRadius: '10px' }}>
        <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
          <Box sx={{ px: 2.5, py: 1.5, borderBottom: '1px solid #f1f5f9', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.9rem' }}>
              Line Items
            </Typography>
            <Chip label={String(invoice.line_items.length)} size="small" sx={{ height: 22, fontSize: '0.75rem', fontWeight: 700, backgroundColor: '#f1f5f9', color: '#475569' }} />
          </Box>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={thSx}>#</TableCell>
                  <TableCell sx={thSx}>Description</TableCell>
                  <TableCell sx={thSx}>HS Code</TableCell>
                  <TableCell sx={thSx} align="right">Qty</TableCell>
                  <TableCell sx={thSx} align="right">Unit Price</TableCell>
                  <TableCell sx={thSx} align="right">Total</TableCell>
                  <TableCell sx={thSx} align="right">Weight</TableCell>
                  <TableCell sx={thSx} align="right">IGST %</TableCell>
                  <TableCell sx={thSx} align="center">Conf</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {invoice.line_items.map((item, idx) => {
                  const confs = [
                    item.description.confidence,
                    item.hs_code_origin.confidence,
                    item.quantity.confidence,
                    item.unit_price_usd.confidence,
                    item.total_price_usd.confidence,
                    item.unit_weight_kg.confidence,
                    item.igst_percent.confidence,
                  ];
                  const avg = confs.reduce((s, c) => s + c, 0) / confs.length;

                  return (
                    <TableRow key={idx} sx={rowHover}>
                      <TableCell sx={{ ...tdSx, color: '#94a3b8', fontWeight: 600, fontSize: '0.75rem' }}>{idx + 1}</TableCell>
                      <TableCell sx={{ ...tdSx, maxWidth: 260, fontWeight: 500 }}>{v(item.description)}</TableCell>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.8rem' }}>{v(item.hs_code_origin)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(item.quantity)}</TableCell>
                      <TableCell sx={tdSx} align="right">{fmtCurrency(item.unit_price_usd.value)}</TableCell>
                      <TableCell sx={tdSx} align="right">{fmtCurrency(item.total_price_usd.value)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(item.unit_weight_kg)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(item.igst_percent)}</TableCell>
                      <TableCell sx={tdSx} align="center">
                        <Chip
                          label={`${(avg * 100).toFixed(0)}%`}
                          color={getConfidenceColor(avg)}
                          size="small"
                          variant="filled"
                          sx={{ height: 22, fontSize: '0.7rem' }}
                        />
                      </TableCell>
                    </TableRow>
                  );
                })}
                {invoice.line_items.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={9} align="center" sx={{ py: 4, color: '#94a3b8', fontSize: '0.85rem' }}>
                      No items extracted
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* ------------------------------------------------------------------ */}
      {/* Boxes table                                                        */}
      {/* ------------------------------------------------------------------ */}
      <Card variant="outlined" sx={{ mb: 3, borderRadius: '10px' }}>
        <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
          <Box sx={{ px: 2.5, py: 1.5, borderBottom: '1px solid #f1f5f9', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.9rem' }}>
              Packing List
            </Typography>
            <Chip label={`${packing_list.boxes.length} boxes`} size="small" sx={{ height: 22, fontSize: '0.75rem', fontWeight: 700, backgroundColor: '#f1f5f9', color: '#475569' }} />
          </Box>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={thSx}>Box #</TableCell>
                  {hasMultiDest && <TableCell sx={thSx}>Dest</TableCell>}
                  <TableCell sx={thSx} align="right">L</TableCell>
                  <TableCell sx={thSx} align="right">W</TableCell>
                  <TableCell sx={thSx} align="right">H</TableCell>
                  <TableCell sx={thSx} align="right">Weight</TableCell>
                  <TableCell sx={thSx}>Items</TableCell>
                  <TableCell sx={thSx} align="center">Conf</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {packing_list.boxes.map((box, idx) => {
                  const itemsSummary = box.items
                    .map((bi) => `${v(bi.description)} \u00d7 ${v(bi.quantity)}`)
                    .join(', ');
                  const confs = [
                    box.box_number.confidence,
                    box.length_cm.confidence,
                    box.width_cm.confidence,
                    box.height_cm.confidence,
                    box.gross_weight_kg.confidence,
                  ];
                  const avg = confs.reduce((s, c) => s + c, 0) / confs.length;

                  return (
                    <TableRow key={idx} sx={rowHover}>
                      <TableCell sx={{ ...tdSx, fontWeight: 600 }}>{v(box.box_number)}</TableCell>
                      {hasMultiDest && (
                        <TableCell sx={tdSx}>
                          {box.destination_id ? (
                            <Chip label={v(box.destination_id)} size="small" sx={{ height: 20, fontSize: '0.7rem', backgroundColor: '#f1f5f9', color: '#475569', fontWeight: 600 }} />
                          ) : '-'}
                        </TableCell>
                      )}
                      <TableCell sx={tdSx} align="right">{v(box.length_cm)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(box.width_cm)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(box.height_cm)}</TableCell>
                      <TableCell sx={tdSx} align="right">{v(box.gross_weight_kg)}</TableCell>
                      <TableCell sx={{ ...tdSx, maxWidth: 280, fontSize: '0.78rem', color: '#64748b' }}>
                        {itemsSummary || '-'}
                      </TableCell>
                      <TableCell sx={tdSx} align="center">
                        <Chip
                          label={`${(avg * 100).toFixed(0)}%`}
                          color={getConfidenceColor(avg)}
                          size="small"
                          variant="filled"
                          sx={{ height: 22, fontSize: '0.7rem' }}
                        />
                      </TableCell>
                    </TableRow>
                  );
                })}
                {packing_list.boxes.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={hasMultiDest ? 8 : 7} align="center" sx={{ py: 4, color: '#94a3b8', fontSize: '0.85rem' }}>
                      No boxes extracted
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* ------------------------------------------------------------------ */}
      {/* Warnings                                                           */}
      {/* ------------------------------------------------------------------ */}
      {result.warnings.length > 0 && (
        <Alert
          severity="warning"
          icon={<WarningAmberOutlined sx={{ fontSize: 20 }} />}
          sx={{ mb: 3, borderRadius: '10px', '& .MuiAlert-message': { fontSize: '0.85rem' } }}
        >
          <AlertTitle sx={{ fontSize: '0.85rem', fontWeight: 600, mb: 0.5 }}>Warnings</AlertTitle>
          {result.warnings.map((w, i) => (
            <Typography key={i} variant="body2" sx={{ fontSize: '0.83rem', color: 'inherit' }}>
              {w}
            </Typography>
          ))}
        </Alert>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Download section                                                   */}
      {/* ------------------------------------------------------------------ */}
      <Divider sx={{ mb: 3 }} />

      <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.9rem', mb: 2 }}>
        Download Sheets
      </Typography>

      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5, mb: 2 }}>
        {downloadTemplates.map((tmpl) => (
          <Paper
            key={tmpl.key}
            onClick={() => onDownload(tmpl.key)}
            sx={{
              flex: '1 1 200px',
              px: 2.5,
              py: 2,
              borderRadius: '10px',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
              '&:hover': {
                borderColor: tmpl.color,
                backgroundColor: '#fafbfc',
                transform: 'translateY(-1px)',
              },
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.75 }}>
              <FileDownloadOutlined sx={{ fontSize: 18, color: tmpl.color }} />
              <Typography sx={{ fontWeight: 600, color: '#1a1a2e', fontSize: '0.85rem' }}>
                {tmpl.name}
              </Typography>
            </Box>
            <Typography sx={{ color: '#94a3b8', fontSize: '0.75rem', lineHeight: 1.4 }}>
              {tmpl.desc}
            </Typography>
          </Paper>
        ))}
      </Box>

      {/* JSON download */}
      <Button
        size="small"
        startIcon={<DataObject sx={{ fontSize: '16px !important' }} />}
        onClick={() => onDownload('result')}
        sx={{
          color: '#94a3b8',
          fontSize: '0.8rem',
          '&:hover': { color: '#64748b', backgroundColor: '#f8fafc' },
        }}
      >
        Download raw JSON
      </Button>

      {/* ------------------------------------------------------------------ */}
      {/* Reset                                                              */}
      {/* ------------------------------------------------------------------ */}
      <Box sx={{ textAlign: 'center', mt: 4, mb: 2 }}>
        <Button
          variant="text"
          startIcon={<RestartAltOutlined sx={{ fontSize: '18px !important' }} />}
          onClick={onReset}
          sx={{
            color: '#94a3b8',
            fontSize: '0.85rem',
            '&:hover': { color: '#475569', backgroundColor: '#f8fafc' },
          }}
        >
          Process New Files
        </Button>
      </Box>
    </Box>
  );
};

export default ResultsSection;
