'use client';
import { useEffect } from 'react';
import Link from 'next/link';
import { AlertTriangle } from 'lucide-react';

// Global error boundary — catches any runtime React errors that bubble up
// (component crashes, unhandled promise rejections in render, etc.)
export default function GlobalError({ error, reset }) {
  useEffect(() => {
    // Optionally log to an error reporting service
    console.error('Unhandled application error:', error);
  }, [error]);

  return (
    <html>
      <body style={{ background: '#0f0f0f', margin: 0, fontFamily: 'Roboto, sans-serif' }}>
        <div
          style={{
            minHeight: '100vh',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '24px',
            padding: '16px',
            textAlign: 'center',
          }}
        >
          <div
            style={{
              width: '80px',
              height: '80px',
              borderRadius: '50%',
              background: 'rgba(255,0,0,0.08)',
              border: '1px solid rgba(255,0,0,0.15)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <AlertTriangle size={36} color="#ff0000" />
          </div>

          <div>
            <p style={{ color: 'rgba(255,0,0,0.7)', fontSize: '11px', fontWeight: 700, letterSpacing: '0.35em', textTransform: 'uppercase', marginBottom: '10px' }}>
              Something went wrong
            </p>
            <h1 style={{ color: '#ffffff', fontSize: '28px', fontWeight: 900, margin: '0 0 8px', letterSpacing: '-0.02em' }}>
              Unexpected Error
            </h1>
            <p style={{ color: 'rgba(255,255,255,0.35)', fontSize: '14px', maxWidth: '320px', lineHeight: 1.6, margin: '0 auto' }}>
              An unexpected error occurred. Try refreshing the page or going back home.
            </p>
          </div>

          <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', justifyContent: 'center' }}>
            <button
              onClick={reset}
              style={{
                padding: '10px 24px',
                borderRadius: '12px',
                fontWeight: 600,
                fontSize: '14px',
                color: '#ffffff',
                background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)',
                boxShadow: '0 4px 20px rgba(255,0,0,0.35)',
                border: 'none',
                cursor: 'pointer',
              }}
            >
              Try Again
            </button>
            <a
              href="/"
              style={{
                padding: '10px 24px',
                borderRadius: '12px',
                fontWeight: 600,
                fontSize: '14px',
                color: 'rgba(255,255,255,0.5)',
                background: 'rgba(255,255,255,0.06)',
                border: '1px solid rgba(255,255,255,0.1)',
                textDecoration: 'none',
              }}
            >
              Back to Home
            </a>
          </div>
        </div>
      </body>
    </html>
  );
}
