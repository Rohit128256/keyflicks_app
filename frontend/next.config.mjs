/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: '/api/:path*',
        destination: 'http://localhost:80/api/:path*',
      },
      {
        source: '/videos/:path*',
        destination: 'http://localhost:80/videos/:path*',
      },
    ]
  },
};

export default nextConfig;
