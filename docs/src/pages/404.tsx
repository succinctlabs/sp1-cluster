import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

const NotFound: React.FC = () => {
  return (
    <Layout title="Page Not Found">
      <div className="container">
        <h1>404 - Page Not Found</h1>
        <p>Sorry, the page you were looking for does not exist.</p>
        <Link
          className="button button--secondary button--lg"
          to="/">
          Home
        </Link>
      </div>
      
    </Layout>
  );
};

export default NotFound;
