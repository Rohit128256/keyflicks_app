import axios from 'axios';
import { useAuthStore } from './store';

// We configure a generic api client.
// Because of Next.js rewrites, '/api' is correctly proxied to 'http://localhost:80/api'
export const api = axios.create({
  baseURL: '/api', 
  withCredentials: true, // IMPORTANT fields for dealing with secure cookies (refresh_token)
});

// Request Interceptor: Attach the access token if we have it
api.interceptors.request.use(
  (config) => {
    const token = useAuthStore.getState().accessToken;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response Interceptor: refresh token on 401
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;
    
    // If the error is 401 and we haven't already tried to refresh the token for this request
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;
      try {
        // Automatically send the secure cookie (refresh_token) to fetch a new token
        const res = await axios.get('/api/refresh-token', {
          withCredentials: true 
        });
        
        let newAccessToken = res.data.access_token;
        if (!newAccessToken && res.data.token) {
             newAccessToken = res.data.token;
        }

        if (newAccessToken) {
          useAuthStore.getState().setAccessToken(newAccessToken);
          // Set the Authorization header for future requests
          api.defaults.headers.common['Authorization'] = `Bearer ${newAccessToken}`;
          // Set the Authorization header for the current failed request
          originalRequest.headers['Authorization'] = `Bearer ${newAccessToken}`;
          // Retry the original request
          return api(originalRequest);
        }
      } catch (refreshError) {
        // If refresh fails, log the user out
        console.error('Refresh token failed/expired', refreshError);
        useAuthStore.getState().logout();
        return Promise.reject(refreshError);
      }
    }

    return Promise.reject(error);
  }
);
