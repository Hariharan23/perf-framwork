require('dotenv').config();

const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Simple, reliable CORS middleware for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  res.header('Access-Control-Allow-Credentials', 'true');
  
  if (req.method === 'OPTIONS') {
    console.log('Handling OPTIONS request for:', req.path);
    res.sendStatus(200);
  } else {
    next();
  }
});

// Security middleware (only in production)
if (process.env.NODE_ENV === 'production') {
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", "'unsafe-inline'", "https://d3js.org", "https://cdn.jsdelivr.net"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", "data:", "https:"],
        connectSrc: ["'self'", "https://*.lambda-url.us-east-1.on.aws", "https://*.execute-api.us-east-1.amazonaws.com"],
        fontSrc: ["'self'", "https:", "data:"],
        objectSrc: ["'none'"],
        mediaSrc: ["'self'"],
        frameSrc: ["'none'"],
      },
    },
    crossOriginEmbedderPolicy: false
  }));
}

// Compression middleware
app.use(compression());

// Logging
app.use(morgan(process.env.NODE_ENV === 'production' ? 'combined' : 'dev'));

// Parse JSON bodies
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Static file serving with caching
app.use(express.static('.', {
  maxAge: process.env.NODE_ENV === 'production' ? '1d' : '0',
  etag: true,
  lastModified: true,
  setHeaders: (res, path) => {
    if (path.endsWith('.html')) {
      // Don't cache HTML files in production for updates
      res.setHeader('Cache-Control', 'no-cache');
    }
  }
}));

// Test endpoint to verify CORS
app.get('/test', (req, res) => {
  console.log('Test endpoint requested');
  res.json({ message: 'CORS test successful', timestamp: new Date().toISOString() });
});

// Health check endpoint
app.get('/health', (req, res) => {
  console.log('Health check requested');
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    service: 'environment-management-frontend',
    version: '1.0.0',
    cors: 'enabled',
    port: PORT,
    environment: process.env.NODE_ENV || 'development'
  });
});

// API proxy endpoints (optional - for additional security/logging)
app.route('/api/query')
  .get(async (req, res) => {
    try {
      res.json({ message: 'Direct Lambda calls are handled by frontend' });
    } catch (error) {
      console.error('API Query error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      res.json({ message: 'Direct Lambda calls are handled by frontend' });
    } catch (error) {
      console.error('API Query error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

app.route('/api/ingest')
  .get(async (req, res) => {
    try {
      res.json({ message: 'Direct Lambda calls are handled by frontend' });
    } catch (error) {
      console.error('API Ingest error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      res.json({ message: 'Direct Lambda calls are handled by frontend' });
    } catch (error) {
      console.error('API Ingest error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// Helper: extract params from GET query string or POST body
function getVisualizationParams(req) {
  return req.method === 'GET' ? req.query : req.body;
}

// 3.1 Load Full Graph — operation: get-network
app.route('/api/visualize/network')
  .get(async (req, res) => {
    try {
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-network' });
    } catch (error) {
      console.error('Visualize network error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-network' });
    } catch (error) {
      console.error('Visualize network error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.2 Load Filtered Sub-Graph — operation: get-network-for-envs
// GET  /api/visualize/network/filtered?envIds=uuid-1,uuid-2&maxHops=2
// POST /api/visualize/network/filtered  { envIds: [...], maxHops: 2 }
app.route('/api/visualize/network/filtered')
  .get(async (req, res) => {
    try {
      const { envIds, maxHops } = req.query;
      const ids = envIds ? envIds.split(',') : [];
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-network-for-envs', envIds: ids, maxHops: maxHops ? Number(maxHops) : 2 });
    } catch (error) {
      console.error('Visualize filtered network error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      const { envIds, maxHops } = req.body;
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-network-for-envs', envIds: envIds || [], maxHops: maxHops || 2 });
    } catch (error) {
      console.error('Visualize filtered network error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.3 Get Environments List — operation: get-environments-list
app.route('/api/visualize/environments')
  .get(async (req, res) => {
    try {
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-environments-list' });
    } catch (error) {
      console.error('Visualize environments error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-environments-list' });
    } catch (error) {
      console.error('Visualize environments error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.4 Get Node Detail — operation: get-entity
// GET  /api/visualize/entity/:id
// POST /api/visualize/entity/:id  (id from path param)
app.route('/api/visualize/entity/:id')
  .get(async (req, res) => {
    try {
      const { id } = req.params;
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-entity', id });
    } catch (error) {
      console.error('Visualize entity error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      const { id } = req.params;
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-entity', id });
    } catch (error) {
      console.error('Visualize entity error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.5 Get Node Change History — operation: get-entity-history
// GET  /api/visualize/entity/:id/history
// POST /api/visualize/entity/:id/history
app.route('/api/visualize/entity/:id/history')
  .get(async (req, res) => {
    try {
      const entityId = req.params.id;
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-entity-history', entityId });
    } catch (error) {
      console.error('Visualize entity history error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      const entityId = req.params.id;
      res.json({ message: 'Proxy to QUERY_LAMBDA_URL', operation: 'get-entity-history', entityId });
    } catch (error) {
      console.error('Visualize entity history error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.6 Reverse Relationship — operation: reverse-relationship
// GET  /api/visualize/relationship/reverse?relationshipId=rel-uuid
// POST /api/visualize/relationship/reverse  { relationshipId: "rel-uuid" }
app.route('/api/visualize/relationship/reverse')
  .get(async (req, res) => {
    try {
      const { relationshipId } = req.query;
      res.json({ message: 'Proxy to INGESTION_LAMBDA_URL', operation: 'reverse-relationship', relationshipId });
    } catch (error) {
      console.error('Visualize reverse relationship error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      const { relationshipId } = req.body;
      res.json({ message: 'Proxy to INGESTION_LAMBDA_URL', operation: 'reverse-relationship', relationshipId });
    } catch (error) {
      console.error('Visualize reverse relationship error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// 3.7 Delete Relationship — operation: delete-relationship
// GET  /api/visualize/relationship/delete?relationshipId=rel-uuid&user=john.doe
// POST /api/visualize/relationship/delete  { relationshipId: "rel-uuid", user: "john.doe" }
app.route('/api/visualize/relationship/delete')
  .get(async (req, res) => {
    try {
      const { relationshipId, user } = req.query;
      res.json({ message: 'Proxy to INGESTION_LAMBDA_URL', operation: 'delete-relationship', relationshipId, user });
    } catch (error) {
      console.error('Visualize delete relationship error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  })
  .post(async (req, res) => {
    try {
      const { relationshipId, user } = req.body;
      res.json({ message: 'Proxy to INGESTION_LAMBDA_URL', operation: 'delete-relationship', relationshipId, user });
    } catch (error) {
      console.error('Visualize delete relationship error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

// Serve the main application
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'environment-management-app.html'));
});

// Handle all other routes by serving the main app (SPA routing)
app.get('*', (req, res) => {
  // Check if it's a file request first
  const ext = path.extname(req.path);
  if (ext) {
    // If it has an extension, it's probably a file request
    res.status(404).json({ error: 'File not found' });
  } else {
    // Otherwise serve the main app
    res.sendFile(path.join(__dirname, 'environment-management-app.html'));
  }
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Server error:', error);
  res.status(500).json({ 
    error: process.env.NODE_ENV === 'production' 
      ? 'Internal server error' 
      : error.message 
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  server.close(() => {
    console.log('Process terminated');
  });
});

const server = app.listen(PORT, () => {
  console.log(`Environment Management System running on port ${PORT}`);
  console.log(`Access the application at: http://localhost:${PORT}`);
  console.log(`Health check available at: http://localhost:${PORT}/health`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});

module.exports = app;