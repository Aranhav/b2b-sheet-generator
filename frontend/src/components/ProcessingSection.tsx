import React from 'react';
import {
  Box,
  Paper,
  Typography,
  LinearProgress,
  Button,
  Alert,
  Stack,
} from '@mui/material';

interface Props {
  progress: number;
  message: string;
  error?: string | null;
  onRetry?: () => void;
}

const ProcessingSection: React.FC<Props> = ({ progress, message, error, onRetry }) => {
  const steps = [
    { label: 'Upload', threshold: 10 },
    { label: 'Extract', threshold: 30 },
    { label: 'Analyze', threshold: 55 },
    { label: 'Generate', threshold: 85 },
  ];

  const activeStep = steps.reduce((acc, step, idx) => (progress >= step.threshold ? idx : acc), 0);

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        width: '100%',
        minHeight: '50vh',
      }}
    >
      <Paper
        sx={{
          p: { xs: 4, sm: 5 },
          maxWidth: 520,
          width: '100%',
          textAlign: 'center',
          borderRadius: '14px',
        }}
      >
        {/* Step indicators */}
        {!error && (
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              gap: 1,
              mb: 4,
            }}
          >
            {steps.map((step, idx) => (
              <Box key={step.label} sx={{ textAlign: 'center', flex: 1 }}>
                <Box
                  sx={{
                    width: 28,
                    height: 28,
                    borderRadius: '50%',
                    mx: 'auto',
                    mb: 0.75,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: '0.7rem',
                    fontWeight: 700,
                    transition: 'all 0.3s ease',
                    backgroundColor:
                      idx < activeStep ? '#1a1a2e' :
                      idx === activeStep ? '#3b82f6' : '#f1f5f9',
                    color:
                      idx <= activeStep ? '#fff' : '#94a3b8',
                  }}
                >
                  {idx < activeStep ? '\u2713' : idx + 1}
                </Box>
                <Typography
                  sx={{
                    fontSize: '0.7rem',
                    fontWeight: idx === activeStep ? 600 : 400,
                    color: idx <= activeStep ? '#334155' : '#94a3b8',
                    transition: 'color 0.3s ease',
                  }}
                >
                  {step.label}
                </Typography>
              </Box>
            ))}
          </Box>
        )}

        {/* Progress bar */}
        <Box sx={{ mb: 3 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 0.5 }}>
            <Box sx={{ flex: 1 }}>
              <LinearProgress
                variant="determinate"
                value={progress}
                sx={{
                  height: 6,
                  borderRadius: 3,
                  backgroundColor: '#f1f5f9',
                  '& .MuiLinearProgress-bar': {
                    borderRadius: 3,
                    backgroundColor: error ? '#ef4444' : '#1a1a2e',
                    transition: 'transform 0.4s ease',
                  },
                }}
              />
            </Box>
            <Typography
              sx={{
                fontWeight: 700,
                color: error ? '#ef4444' : '#1a1a2e',
                fontSize: '0.8rem',
                minWidth: 40,
                textAlign: 'right',
                fontVariantNumeric: 'tabular-nums',
              }}
            >
              {Math.round(progress)}%
            </Typography>
          </Box>
        </Box>

        {/* Status message */}
        <Typography
          sx={{
            color: error ? '#ef4444' : '#64748b',
            fontWeight: 500,
            fontSize: '0.85rem',
            mb: error ? 3 : 0,
          }}
        >
          {message}
        </Typography>

        {/* Error */}
        {error && (
          <Stack spacing={2} sx={{ mt: 2 }}>
            <Alert
              severity="error"
              sx={{
                textAlign: 'left',
                borderRadius: '10px',
                '& .MuiAlert-message': { width: '100%' },
              }}
            >
              <Typography variant="body2" sx={{ fontWeight: 500, fontSize: '0.85rem' }}>
                {error}
              </Typography>
            </Alert>
            {onRetry && (
              <Button
                variant="contained"
                onClick={onRetry}
                sx={{
                  alignSelf: 'center',
                  px: 4,
                  py: 1,
                  backgroundColor: '#1a1a2e',
                  '&:hover': { backgroundColor: '#2d2d4a' },
                }}
              >
                Try Again
              </Button>
            )}
          </Stack>
        )}
      </Paper>
    </Box>
  );
};

export default ProcessingSection;
