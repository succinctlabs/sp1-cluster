import React from 'react';
import Navbar from '@theme-original/Navbar';
import type NavbarType from '@theme/Navbar';
import type {WrapperProps} from '@docusaurus/types';

type Props = WrapperProps<typeof NavbarType>;

export default function NavbarWrapper(props: Props): JSX.Element {
  // Seeded random number generator
  const seededRandom = (function() {
    let seed = 42; // You can change this seed value
    return function() {
      seed = (seed * 16807) % 2147483647;
      return (seed - 1) / 2147483646;
    };
  })();

  const generateBarcodeStripes = () => {
    const stripes = [];
    let position = 0;
    
    while (position < 100) {
      const isThick = seededRandom() < 0.3;
      
      if (isThick) {
        const width = seededRandom() * 2 + 3;
        const gap = seededRandom() * 2 + 3;
        
        stripes.push(
          <div
            key={position}
            style={{
              position: 'absolute',
              left: `${position}%`,
              width: `${width}%`,
              height: '100%',
              backgroundColor: '#90DCFF',
            }}
          />
        );
        
        position += width + gap;
      } else {
        const numThinStripes = Math.floor(seededRandom() * 3) + 2;
        
        for (let i = 0; i < numThinStripes; i++) {
          const width = seededRandom() * 0.2 + 0.1;
          const gap = seededRandom() * 0.3 + 0.3;
          
          stripes.push(
            <div
              key={`${position}-${i}`}
              style={{
                position: 'absolute',
                left: `${position}%`,
                width: `${width}%`,
                height: '100%',
                backgroundColor: '#90DCFF',
              }}
            />
          );
          
          position += width + gap;
        }
        
        position += 2;
      }
    }
    return stripes;
  };

  return (
    <>
      <div 
        style={{
          height: '20px',
          position: 'relative',
          overflow: 'hidden',
          padding: '0px',
          margin: '0px',
        }}
      >
        {generateBarcodeStripes()}
      </div>
      
      <div style={{height: '10px'}}> </div>
        <Navbar {...props} />
    </>
  );
}
