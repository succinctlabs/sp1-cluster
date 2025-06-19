import { useEffect, type ReactNode } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  // Redirect to /cluster
  useEffect(() => {
    window.location.href = '/docs/cluster/introduction';
  }, []);
  return (
    // <header className={clsx('hero hero--primary', styles.heroBanner)}>
    //   <div className="container">
    //     <div className={styles.heroContent}>
    //       <div className={styles.heroText}>
    //         <Heading as="h1" className="hero__title">
    //           <span style={{ color: '#FE01AC' }}>Prove</span> the world's software
    //         </Heading>
    //         <p className="hero__subtitle" style={{ letterSpacing: '-0.02em' }}>
    //           Generate proofs at light speed with SP1 and our decentralized prover network.
    //         </p>
    //         <div className={styles.buttons}>
    //           <Link
    //             className="button button--secondary button--lg"
    //             to="/docs/network/introduction">
    //             Network
    //           </Link>
    //           {/* <Link
    //             className="button button--secondary button--lg"
    //             to="/docs/developers/intro">
    //             Developers
    //           </Link> */}
    //           <Link
    //             className="button button--secondary button--lg"
    //             to="/docs/sp1/introduction">
    //             SP1
    //           </Link>
    //         </div>
    //       </div>
    //       <div className={styles.heroGraphic}>
    //         <img
    //           src="/img/square.svg"
    //           alt="Square graphic"
    //           width="800"
    //           height="800"
    //           className={styles.heroImage}
    //         />
    //       </div>
    //     </div>
    //   </div>
    // </header>
    null
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={``}
      description="The documentation for SP1 and the Succinct prover network">
      <HomepageHeader />
    </Layout>
  );
}
