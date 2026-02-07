import { useState, useRef, useCallback } from 'react';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';
import { Box, Typography, Container, Chip, Snackbar, Alert } from '@mui/material';
import UploadSection from './components/UploadSection';
import ProcessingSection from './components/ProcessingSection';
import ResultsSection from './components/ResultsSection';
import { extractDocuments, getDownloadUrl } from './api';
import type { ExtractionResult, ExtractionOptions } from './types/extraction';

// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------
const theme = createTheme({
  palette: {
    primary: { main: '#1a1a2e' },
    secondary: { main: '#3b82f6' },
    background: { default: '#fafafa', paper: '#ffffff' },
    text: { primary: '#1a1a2e', secondary: '#64748b' },
  },
  typography: {
    fontFamily: 'Inter, system-ui, -apple-system, sans-serif',
    h5: { fontWeight: 700, letterSpacing: '-0.02em' },
    h6: { fontWeight: 600, letterSpacing: '-0.01em' },
    subtitle1: { fontWeight: 600 },
    body2: { color: '#64748b' },
  },
  shape: { borderRadius: 10 },
  components: {
    MuiPaper: {
      defaultProps: { elevation: 0 },
      styleOverrides: {
        root: {
          border: '1px solid #e8ecf1',
          backgroundImage: 'none',
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none' as const,
          fontWeight: 600,
          borderRadius: 8,
          boxShadow: 'none',
          '&:hover': { boxShadow: 'none' },
        },
        contained: {
          '&:hover': { boxShadow: '0 1px 3px rgba(0,0,0,0.1)' },
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          border: '1px solid #e8ecf1',
          boxShadow: 'none',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: { fontWeight: 600, fontSize: '0.75rem' },
      },
    },
  },
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
type ViewState = 'upload' | 'processing' | 'results';

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------
function App() {
  const [view, setView] = useState<ViewState>('upload');
  const [files, setFiles] = useState<File[]>([]);
  const [lastOptions, setLastOptions] = useState<ExtractionOptions | undefined>();
  const [progress, setProgress] = useState(0);
  const [progressMessage, setProgressMessage] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ExtractionResult | null>(null);
  const [jobId, setJobId] = useState('');
  const [toast, setToast] = useState<{ open: boolean; message: string; severity: 'error' | 'warning' | 'success' }>({
    open: false,
    message: '',
    severity: 'error',
  });

  const progressIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const clearProgressInterval = useCallback(() => {
    if (progressIntervalRef.current !== null) {
      clearInterval(progressIntervalRef.current);
      progressIntervalRef.current = null;
    }
  }, []);

  const startProgressSimulation = useCallback(() => {
    clearProgressInterval();
    const steps = [
      { target: 10, message: 'Uploading files...', delay: 0 },
      { target: 30, message: 'Extracting text from PDFs...', delay: 1500 },
      { target: 55, message: 'Analyzing invoice with AI...', delay: 4000 },
      { target: 70, message: 'Extracting packing list data...', delay: 7000 },
      { target: 85, message: 'Generating Excel sheets...', delay: 12000 },
    ];
    const startTime = Date.now();

    progressIntervalRef.current = setInterval(() => {
      const elapsed = Date.now() - startTime;
      let currentStep = steps[0];
      for (const step of steps) {
        if (elapsed >= step.delay) currentStep = step;
      }
      setProgress((prev) => {
        if (prev < currentStep.target) {
          const increment = Math.max(0.3, (currentStep.target - prev) * 0.06);
          return Math.min(currentStep.target, prev + increment);
        }
        return prev;
      });
      setProgressMessage(currentStep.message);
    }, 150);
  }, [clearProgressInterval]);

  const handleProcess = useCallback(
    async (selectedFiles: File[], options?: ExtractionOptions) => {
      setFiles(selectedFiles);
      setLastOptions(options);
      setView('processing');
      setProgress(0);
      setError(null);
      setResult(null);
      setJobId('');
      startProgressSimulation();

      try {
        const jobStatus = await extractDocuments(selectedFiles, options);
        clearProgressInterval();

        const extractionResult = jobStatus.result;

        // Check if the extraction completely failed (no data at all)
        if (extractionResult?.status === 'failed') {
          const errorMsgs = [
            ...(extractionResult.errors || []),
            ...(extractionResult.warnings || []),
          ];
          const displayMsg = errorMsgs.length > 0
            ? errorMsgs.join(' | ')
            : 'Extraction failed. Please try again.';
          setError(displayMsg);
          setProgressMessage('Processing failed.');
          setToast({ open: true, message: displayMsg, severity: 'error' });
          return;
        }

        setProgress(100);
        setProgressMessage('Done!');
        setJobId(jobStatus.job_id);
        setResult(extractionResult);

        // Show warnings as toast if any
        if (extractionResult?.warnings && extractionResult.warnings.length > 0) {
          setToast({
            open: true,
            message: extractionResult.warnings.join(' | '),
            severity: 'warning',
          });
        }

        setTimeout(() => setView('results'), 500);
      } catch (err) {
        clearProgressInterval();
        const message = err instanceof Error ? err.message : 'An unexpected error occurred.';
        setError(message);
        setProgressMessage('Processing failed.');
        setToast({ open: true, message, severity: 'error' });
      }
    },
    [startProgressSimulation, clearProgressInterval]
  );

  const handleRetry = useCallback(() => {
    if (files.length > 0) {
      handleProcess(files, lastOptions);
    } else {
      setView('upload');
      setError(null);
      setProgress(0);
      setProgressMessage('');
    }
  }, [files, lastOptions, handleProcess]);

  const handleDownload = useCallback(
    (type: 'multi' | 'simplified' | 'b2b_shipment' | 'result') => {
      if (!jobId) return;
      window.open(getDownloadUrl(jobId, type), '_blank');
    },
    [jobId]
  );

  const handleReset = useCallback(() => {
    clearProgressInterval();
    setView('upload');
    setFiles([]);
    setProgress(0);
    setProgressMessage('');
    setError(null);
    setResult(null);
    setJobId('');
    setLastOptions(undefined);
  }, [clearProgressInterval]);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />

      {/* Header */}
      <Box
        sx={{
          borderBottom: '1px solid #e8ecf1',
          backgroundColor: '#fff',
          position: 'sticky',
          top: 0,
          zIndex: 1100,
        }}
      >
        <Container maxWidth="lg">
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              height: 64,
            }}
          >
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg">
                <rect width="32" height="32" rx="8" fill="url(#logo-grad)" />
                <path d="M8 22V10h4.5c1.2 0 2.1.3 2.7.8.6.5.9 1.2.9 2 0 .6-.2 1.1-.5 1.5-.3.4-.8.7-1.3.8v.1c.7.1 1.2.4 1.6.9.4.5.6 1.1.6 1.8 0 .9-.3 1.7-1 2.2-.6.5-1.5.8-2.7.8H8zm2.4-7h2c.6 0 1-.1 1.3-.4.3-.3.5-.6.5-1.1 0-.4-.2-.8-.5-1-.3-.3-.7-.4-1.3-.4h-2V15zm0 5h2.2c.6 0 1.1-.2 1.4-.5.3-.3.5-.7.5-1.2 0-.5-.2-.9-.5-1.2-.3-.3-.8-.4-1.4-.4h-2.2V20z" fill="#fff"/>
                <path d="M18 22V10h2.4v5.2h.1l3.6-5.2H26.8l-3.9 5.4L27 22h-2.8l-3.4-5.1h-.1V22H18z" fill="#fff" opacity="0.7"/>
                <defs>
                  <linearGradient id="logo-grad" x1="0" y1="0" x2="32" y2="32" gradientUnits="userSpaceOnUse">
                    <stop stopColor="#1a1a2e"/>
                    <stop offset="1" stopColor="#3b82f6"/>
                  </linearGradient>
                </defs>
              </svg>
              <Typography
                variant="h6"
                sx={{
                  fontSize: '1.05rem',
                  fontWeight: 700,
                  color: '#1a1a2e',
                  letterSpacing: '-0.02em',
                }}
              >
                B2B Sheet Generator
              </Typography>
            </Box>

            <Chip
              label={
                view === 'upload' ? 'Upload' :
                view === 'processing' ? 'Processing' : 'Results'
              }
              size="small"
              sx={{
                backgroundColor:
                  view === 'results' ? '#ecfdf5' :
                  view === 'processing' ? '#eff6ff' : '#f8fafc',
                color:
                  view === 'results' ? '#059669' :
                  view === 'processing' ? '#3b82f6' : '#64748b',
                fontWeight: 600,
                fontSize: '0.75rem',
              }}
            />
          </Box>
        </Container>
      </Box>

      {/* Main */}
      <Container maxWidth="lg" sx={{ py: 4 }}>
        {view === 'upload' && (
          <UploadSection onFilesSelected={handleProcess} isProcessing={false} />
        )}
        {view === 'processing' && (
          <ProcessingSection
            progress={progress}
            message={progressMessage}
            error={error}
            onRetry={handleRetry}
          />
        )}
        {view === 'results' && result && (
          <ResultsSection
            result={result}
            jobId={jobId}
            onDownload={handleDownload}
            onReset={handleReset}
          />
        )}
      </Container>

      {/* Footer */}
      <Box
        component="footer"
        sx={{
          textAlign: 'center',
          py: 3,
          color: '#94a3b8',
          fontSize: '0.8rem',
        }}
      >
        <Typography variant="caption" color="inherit">
          Upload invoices & packing lists. Get formatted XpressB2B sheets.
        </Typography>
      </Box>
      {/* Toast notification */}
      <Snackbar
        open={toast.open}
        autoHideDuration={toast.severity === 'error' ? 8000 : 5000}
        onClose={() => setToast((prev) => ({ ...prev, open: false }))}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert
          onClose={() => setToast((prev) => ({ ...prev, open: false }))}
          severity={toast.severity}
          variant="filled"
          sx={{
            width: '100%',
            maxWidth: 600,
            fontSize: '0.85rem',
            fontWeight: 500,
            borderRadius: '10px',
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
          }}
        >
          {toast.message}
        </Alert>
      </Snackbar>
    </ThemeProvider>
  );
}

export default App;
