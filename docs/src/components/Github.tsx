import React, { useEffect, useState, useRef } from 'react';
import type { JSX } from 'react';
import CodeBlock from '@theme/CodeBlock';

interface GithubProps {
  url: string;
  start?: number;
  end?: number;
  language: string;
  fname?: string;
  metastring?: string;
  withSourceLink?: boolean;
  scrollable?: boolean;
  mode?: 'full' | 'trimmed';
}

function toRaw(ref: string): string {
  const fullUrl = ref.slice(ref.indexOf('https'));
  const [url] = fullUrl.split('#');
  const [org, repo, blob, branch, ...pathSeg] = new URL(url).pathname.split('/').slice(1);
  return `https://raw.githubusercontent.com/${org}/${repo}/${branch}/${pathSeg.join('/')}`;
}

async function fetchCode(url: string, fromLine?: number, toLine?: number): Promise<string> {
  let res: string;

  // check if stored in cache
  const validUntil = localStorage.getItem(`${url}-until`);

  if (validUntil && parseInt(validUntil) > Date.now()) {
    res = localStorage.getItem(url) || '';
  } else {
    try {
      const response = await fetch(url);
      res = await response.text();
      localStorage.setItem(url, res);
      localStorage.setItem(`${url}-until`, (Date.now() + 60000).toString());
    } catch {
      return 'Error fetching code, please try reloading';
    }
  }

  let body = res.split('\n');
  fromLine = fromLine ? Number(fromLine) - 1 : 0;
  toLine = toLine ? Number(toLine) : body.length;
  body = body.slice(fromLine, toLine);

  // Remove indentation on nested code
  const preceedingSpace = body.reduce((prev, line) => {
    if (line.length === 0) return prev;

    const spaces = line.match(/^\s+/);
    if (spaces) return Math.min(prev, spaces[0].length);

    return 0;
  }, Infinity);

  return body.map((line) => line.slice(preceedingSpace)).join('\n');
}

export function Github({
  url,
  start,
  end,
  language,
  fname,
  metastring = '',
  withSourceLink = true,
  scrollable = true,
  mode = 'trimmed',
}: GithubProps): JSX.Element {
  const [code, setCode] = useState('Loading...');
  const [isSmall, setIsSmall] = useState(false);
  const [isLarge, setIsLarge] = useState(false);
  const codeRef = useRef<HTMLDivElement>(null);

  // Constants for styling
  const SMALL_MARGIN = '0px';
  const NORMAL_MARGIN = '10px';
  const LARGE_MARGIN = '18px';
  const MAX_HEIGHT = '400px';

  useEffect(() => {
    const rawUrl = toRaw(url);
    fetchCode(rawUrl, start, end).then((res) => setCode(res));
  }, [url, start, end]);

  useEffect(() => {
    if (codeRef.current) {
      const height = codeRef.current.scrollHeight;
      setIsSmall(height < 100); // Consider snippets under 100px as small
      setIsLarge(height > 300); // Consider snippets over 300px as large
    }
  }, [code]);

  // Determine if we should apply scrolling
  const shouldScroll = mode === 'trimmed' && scrollable && isLarge;

  // Determine margin based on size and scrollability
  const getMargin = () => {
    if (isSmall) return SMALL_MARGIN;
    if (shouldScroll) return LARGE_MARGIN;
    return NORMAL_MARGIN;
  };

  return (
    <div data-fname={fname}>
      <div 
        style={{ 
          position: 'relative'
        }}
      >
        {withSourceLink && (
          <div
            style={{
              position: 'absolute',
              top: '8px',
              right: '8px',
              zIndex: 1,
              fontSize: '0.9em',
              fontWeight: 300,
              padding: '2px 6px 2px 6px',
              borderRadius: '4px',
            }}
          >
            <a href={url} target="_blank" rel="noreferrer noopener">
              Source
            </a>
          </div>
        )}
        <div 
          ref={codeRef}
          style={shouldScroll ? 
            { maxHeight: MAX_HEIGHT, overflow: 'auto', marginBottom: getMargin() } : 
            { marginBottom: getMargin() }
          }
        >
          <div className="hide-buttons-wrapper" style={{ position: 'relative' }}>
            <style>{`
              .hide-buttons-wrapper .clean-btn {
                display: none !important;
              }
              .hide-buttons-wrapper .theme-code-block-highlighted-line {
                display: none !important;
              }
            `}</style>
            <CodeBlock 
              language={language} 
              metastring={metastring}
            >
              {code}
            </CodeBlock>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Github;
