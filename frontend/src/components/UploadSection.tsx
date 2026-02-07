import React, { useRef, useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  IconButton,
  Stack,
  Alert,
  Select,
  MenuItem,
  TextField,
  Switch,
  FormControlLabel,
  Collapse,
} from '@mui/material';
import {
  CloudUploadOutlined,
  Close,
  InsertDriveFileOutlined,
  ArrowForward,
  TuneOutlined,
} from '@mui/icons-material';
import type { ExtractionOptions } from '../types/extraction';

interface Props {
  onFilesSelected: (files: File[], options: ExtractionOptions) => void;
  isProcessing: boolean;
}

interface FileWithId {
  file: File;
  id: string;
}

const DEFAULT_OPTIONS: ExtractionOptions = {
  output_currency: 'auto',
  exchange_rate: null,
  sync_hs_codes: true,
};

const UploadSection: React.FC<Props> = ({ onFilesSelected, isProcessing }) => {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [files, setFiles] = useState<FileWithId[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [options, setOptions] = useState<ExtractionOptions>(DEFAULT_OPTIONS);
  const [showSettings, setShowSettings] = useState(false);

  const MAX_FILE_SIZE = 20 * 1024 * 1024;
  const ALLOWED_FILE_TYPE = 'application/pdf';

  const validateFiles = (filesToValidate: File[]): FileWithId[] => {
    const errors: string[] = [];
    const validFiles: FileWithId[] = [];

    filesToValidate.forEach((file) => {
      if (file.type !== ALLOWED_FILE_TYPE) {
        errors.push(`${file.name} - Only PDF files are accepted.`);
      } else if (file.size > MAX_FILE_SIZE) {
        errors.push(`${file.name} - Exceeds 20 MB limit (${formatFileSize(file.size)})`);
      } else {
        validFiles.push({
          file,
          id: `${file.name}-${Date.now()}-${Math.random()}`,
        });
      }
    });

    if (errors.length > 0) setValidationErrors(errors);
    return validFiles;
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 10) / 10 + ' ' + sizes[i];
  };

  const handleDragEnter = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(true);
  };

  const handleDragLeave = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(true);
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    setValidationErrors([]);
    const droppedFiles = Array.from(e.dataTransfer.files);
    const validatedFiles = validateFiles(droppedFiles);
    setFiles((prev) => [...prev, ...validatedFiles]);
  };

  const handleBrowseClick = () => fileInputRef.current?.click();

  const handleFileInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setValidationErrors([]);
    const selectedFiles = Array.from(e.target.files || []);
    const validatedFiles = validateFiles(selectedFiles);
    setFiles((prev) => [...prev, ...validatedFiles]);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const handleRemoveFile = (id: string) => {
    setFiles((prev) => prev.filter((f) => f.id !== id));
  };

  const handleProcessFiles = () => {
    onFilesSelected(files.map((f) => f.file), options);
  };

  const showExchangeRate = options.output_currency !== 'auto';

  return (
    <Box sx={{ maxWidth: 680, mx: 'auto' }}>
      {/* Page heading */}
      <Box sx={{ textAlign: 'center', mb: 4 }}>
        <Typography
          variant="h5"
          sx={{ mb: 1, color: '#1a1a2e', fontSize: '1.5rem' }}
        >
          Upload Documents
        </Typography>
        <Typography variant="body2" sx={{ color: '#94a3b8', fontSize: '0.9rem' }}>
          Drop your invoice and packing list PDFs to generate formatted B2B sheets
        </Typography>
      </Box>

      {/* Drop zone */}
      <Paper
        onDragEnter={handleDragEnter}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onClick={handleBrowseClick}
        sx={{
          border: dragActive ? '2px solid #3b82f6' : '2px dashed #d1d5db',
          backgroundColor: dragActive ? '#f0f7ff' : '#fafbfc',
          borderRadius: '12px',
          padding: 5,
          textAlign: 'center',
          cursor: 'pointer',
          transition: 'all 0.2s ease',
          '&:hover': {
            borderColor: '#94a3b8',
            backgroundColor: '#f8f9fa',
          },
        }}
      >
        <Box
          sx={{
            width: 56,
            height: 56,
            borderRadius: '14px',
            backgroundColor: dragActive ? '#dbeafe' : '#f1f5f9',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            mx: 'auto',
            mb: 2,
            transition: 'all 0.2s ease',
          }}
        >
          <CloudUploadOutlined
            sx={{
              fontSize: 28,
              color: dragActive ? '#3b82f6' : '#94a3b8',
              transition: 'color 0.2s ease',
            }}
          />
        </Box>
        <Typography
          sx={{
            fontWeight: 600,
            color: '#334155',
            mb: 0.5,
            fontSize: '0.95rem',
          }}
        >
          {dragActive ? 'Drop files here' : 'Click to upload or drag and drop'}
        </Typography>
        <Typography sx={{ color: '#94a3b8', fontSize: '0.8rem' }}>
          PDF files only, up to 20 MB each
        </Typography>
        <input
          ref={fileInputRef}
          type="file"
          accept=".pdf"
          multiple
          onChange={handleFileInputChange}
          style={{ display: 'none' }}
        />
      </Paper>

      {/* Validation errors */}
      {validationErrors.length > 0 && (
        <Alert
          severity="error"
          sx={{ mt: 2, borderRadius: '10px', fontSize: '0.85rem' }}
          onClose={() => setValidationErrors([])}
        >
          <Stack spacing={0.25}>
            {validationErrors.map((err, i) => (
              <Typography key={i} variant="body2" sx={{ fontSize: '0.85rem', color: 'inherit' }}>
                {err}
              </Typography>
            ))}
          </Stack>
        </Alert>
      )}

      {/* File list + settings */}
      {files.length > 0 && (
        <Box sx={{ mt: 3 }}>
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              mb: 1.5,
            }}
          >
            <Typography
              sx={{ fontWeight: 600, color: '#334155', fontSize: '0.85rem' }}
            >
              {files.length} file{files.length !== 1 ? 's' : ''} selected
            </Typography>
            {files.length > 1 && (
              <Button
                size="small"
                onClick={() => { setFiles([]); setValidationErrors([]); }}
                sx={{
                  color: '#94a3b8',
                  fontSize: '0.8rem',
                  minWidth: 'auto',
                  '&:hover': { color: '#ef4444', backgroundColor: 'transparent' },
                }}
              >
                Clear all
              </Button>
            )}
          </Box>

          <Stack spacing={1}>
            {files.map((fw) => (
              <Paper
                key={fw.id}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  px: 2,
                  py: 1.25,
                  borderRadius: '10px',
                  backgroundColor: '#fafbfc',
                }}
              >
                <InsertDriveFileOutlined
                  sx={{ fontSize: 20, color: '#ef4444', mr: 1.5 }}
                />
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                    sx={{
                      fontWeight: 500,
                      color: '#334155',
                      fontSize: '0.85rem',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {fw.file.name}
                  </Typography>
                  <Typography sx={{ color: '#94a3b8', fontSize: '0.75rem' }}>
                    {formatFileSize(fw.file.size)}
                  </Typography>
                </Box>
                <IconButton
                  size="small"
                  onClick={() => handleRemoveFile(fw.id)}
                  sx={{
                    color: '#cbd5e1',
                    '&:hover': { color: '#ef4444', backgroundColor: '#fef2f2' },
                  }}
                >
                  <Close sx={{ fontSize: 18 }} />
                </IconButton>
              </Paper>
            ))}
          </Stack>

          {/* Settings toggle */}
          <Button
            size="small"
            startIcon={<TuneOutlined sx={{ fontSize: '16px !important' }} />}
            onClick={() => setShowSettings((v) => !v)}
            sx={{
              mt: 2,
              color: '#64748b',
              fontSize: '0.8rem',
              fontWeight: 500,
              '&:hover': { backgroundColor: '#f1f5f9' },
            }}
          >
            {showSettings ? 'Hide options' : 'Export options'}
          </Button>

          {/* Settings panel */}
          <Collapse in={showSettings}>
            <Paper
              sx={{
                mt: 1,
                p: 2.5,
                borderRadius: '10px',
                backgroundColor: '#f8fafc',
                border: '1px solid #e2e8f0',
              }}
            >
              <Stack spacing={2.5}>
                {/* Output Currency */}
                <Box>
                  <Typography sx={{ fontSize: '0.8rem', fontWeight: 600, color: '#334155', mb: 0.75 }}>
                    Output Currency
                  </Typography>
                  <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8', mb: 1 }}>
                    Currency for prices in the generated sheets
                  </Typography>
                  <Select
                    size="small"
                    value={options.output_currency}
                    onChange={(e) =>
                      setOptions((prev) => ({
                        ...prev,
                        output_currency: e.target.value as ExtractionOptions['output_currency'],
                        exchange_rate: e.target.value === 'auto' ? null : prev.exchange_rate,
                      }))
                    }
                    sx={{
                      fontSize: '0.85rem',
                      minWidth: 180,
                      backgroundColor: '#fff',
                      '& .MuiSelect-select': { py: 1 },
                    }}
                  >
                    <MenuItem value="auto">Auto-detect from invoice</MenuItem>
                    <MenuItem value="USD">USD (US Dollar)</MenuItem>
                    <MenuItem value="INR">INR (Indian Rupee)</MenuItem>
                  </Select>
                </Box>

                {/* Exchange Rate - shown when currency is not auto */}
                <Collapse in={showExchangeRate}>
                  <Box>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 600, color: '#334155', mb: 0.75 }}>
                      Exchange Rate
                    </Typography>
                    <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8', mb: 1 }}>
                      {options.output_currency === 'INR'
                        ? 'How many INR per 1 USD (e.g. 83.5)'
                        : 'How many USD per 1 INR (e.g. 0.012)'}
                    </Typography>
                    <TextField
                      size="small"
                      type="number"
                      placeholder={options.output_currency === 'INR' ? '83.50' : '0.012'}
                      value={options.exchange_rate ?? ''}
                      onChange={(e) =>
                        setOptions((prev) => ({
                          ...prev,
                          exchange_rate: e.target.value ? parseFloat(e.target.value) : null,
                        }))
                      }
                      inputProps={{ step: 0.01, min: 0 }}
                      sx={{
                        width: 180,
                        backgroundColor: '#fff',
                        '& .MuiInputBase-input': { fontSize: '0.85rem', py: 1 },
                      }}
                    />
                  </Box>
                </Collapse>

                {/* HS Code Sync */}
                <Box>
                  <FormControlLabel
                    control={
                      <Switch
                        size="small"
                        checked={options.sync_hs_codes}
                        onChange={(e) =>
                          setOptions((prev) => ({ ...prev, sync_hs_codes: e.target.checked }))
                        }
                        sx={{
                          '& .MuiSwitch-switchBase.Mui-checked': { color: '#1a1a2e' },
                          '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
                            backgroundColor: '#1a1a2e',
                          },
                        }}
                      />
                    }
                    label={
                      <Typography sx={{ fontSize: '0.8rem', fontWeight: 600, color: '#334155' }}>
                        Sync HS Codes
                      </Typography>
                    }
                  />
                  <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8', ml: 5.8 }}>
                    If origin or destination HS code is missing, copy from the other
                  </Typography>
                </Box>
              </Stack>
            </Paper>
          </Collapse>

          {/* Process button */}
          <Button
            variant="contained"
            fullWidth
            onClick={handleProcessFiles}
            disabled={isProcessing}
            endIcon={<ArrowForward sx={{ fontSize: '18px !important' }} />}
            sx={{
              mt: 3,
              py: 1.5,
              fontSize: '0.9rem',
              fontWeight: 600,
              backgroundColor: '#1a1a2e',
              borderRadius: '10px',
              '&:hover': {
                backgroundColor: '#2d2d4a',
              },
            }}
          >
            {isProcessing
              ? 'Processing...'
              : `Extract & Generate Sheets`}
          </Button>
        </Box>
      )}

      {/* Help text */}
      {files.length === 0 && (
        <Box
          sx={{
            mt: 4,
            display: 'flex',
            gap: 3,
            justifyContent: 'center',
            flexWrap: 'wrap',
          }}
        >
          {[
            { label: 'Invoice', desc: 'Line items, prices, HS codes' },
            { label: 'Packing List', desc: 'Box dims, weights, contents' },
          ].map((item) => (
            <Box
              key={item.label}
              sx={{
                textAlign: 'center',
                flex: '0 1 200px',
              }}
            >
              <Typography
                sx={{
                  fontWeight: 600,
                  color: '#475569',
                  fontSize: '0.8rem',
                  mb: 0.25,
                }}
              >
                {item.label}
              </Typography>
              <Typography sx={{ color: '#94a3b8', fontSize: '0.75rem' }}>
                {item.desc}
              </Typography>
            </Box>
          ))}
        </Box>
      )}
    </Box>
  );
};

export default UploadSection;
