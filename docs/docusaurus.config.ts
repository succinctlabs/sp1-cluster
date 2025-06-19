import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Succinct Docs',
  tagline: 'Prove the world\'s software',
  favicon: 'img/favicon.svg',

  // Set the production url of your site here
  url: 'https://docs.succinct.xyz',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'succinctlabs', // Usually your GitHub org/user name.
  projectName: 'succinct', // Usually your repo name.

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  
  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
        },
        // blog: {
        //   showReadingTime: true,
        //   feedOptions: {
        //     type: ['rss', 'atom'],
        //     xslt: true,
        //   },
        //   // Please change this to your repo.
        //   // Remove this to remove the "edit this page" links.
        //   editUrl:
        //     'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        //   // Useful options to enforce blogging best practices
        //   onInlineTags: 'warn',
        //   onInlineAuthors: 'warn',
        //   onUntruncatedBlogPosts: 'warn',
        // },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
     algolia: {
      apiKey: "8bfb4b393679faa73e8362e3966be8c3", // Public api key
      appId: "P3LCHD8MFM",
      indexName: "succinct",
      searchPagePath: "search",

      // Leaving at the default of `true` for now
      contextualSearch: true,
    },
    image: 'img/Cover.jpg',
    metadata: [
      {name: 'twitter:card', content: 'summary_large_image'},
      {name: 'twitter:site', content: '@yourtwitterhandle'},
      {name: 'og:type', content: 'website'},
    ],
    navbar: {
      title: 'Succinct Docs',
      logo: {
        alt: 'Succinct Logo',
        src: 'img/logo-black.svg',
      },
      items: [
        // {
        //   type: 'docSidebar',
        //   sidebarId: 'succinctSidebar',
        //   position: 'left',
        //   label: 'Network',
        // },
        // {
        //   type: 'docSidebar',
        //   sidebarId: 'sp1Sidebar',
        //   position: 'left',
        //   label: 'SP1',
        // },
        {
          type: 'docSidebar',
          sidebarId: 'clusterSidebar',
          position: 'left',
          label: 'Cluster',
        },
        {
          href: 'https://explorer.sepolia.succinct.xyz',
          label: 'Explorer (Sepolia)',
          position: 'right',
        },
        {
          href: 'https://staking.sepolia.succinct.xyz',
          label: 'Staking (Sepolia)',
          position: 'right',
        },
        {
          href: 'https://succinct.xyz',
          label: 'Website',
          position: 'right',
        },
        {
          href: 'https://blog.succinct.xyz',
          label: 'Blog',
          position: 'right',
        },
        {
          href: 'https://github.com/succinctlabs',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://x.com/succinctlabs',
          label: 'X',
          position: 'right',
        },
        {
          href: 'https://discord.gg/succinctlabs',
          label: 'Discord',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Resources',
          items: [
            {
              label: 'Website',
              href: 'https://succinct.xyz',
            },
            {
              label: 'Blog',
              href: 'https://blog.succinct.xyz',
            },
            {
              label: 'Explorer',
              href: 'https://network.succinct.xyz',
            },
            {
              label: 'Testnet',
              href: 'https://testnet.succinct.xyz',
            },
            {
              label: 'Whitepaper',
              href: 'https://provewith.us',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'X',
              href: 'https://x.com/succinctlabs',
            },
            {
              label: 'Discord',
              href: 'https://discord.gg/succinctlabs',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Succinct Labs`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['solidity'],
    },
    colorMode: {
      disableSwitch: true,
      defaultMode: 'light',
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
