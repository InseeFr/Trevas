import React, { memo } from 'react';
import BrowserOnly from '@docusaurus/BrowserOnly';

export type Props = {
	script: string;
};

const Editor = memo((props: Props) => {
	const { script } = props;
	return (
		<BrowserOnly>
			{() => {
				// eslint-disable-next-line @typescript-eslint/no-var-requires
				const VTLEditor = require('@making-sense/antlr-editor').AntlrEditor;
				// eslint-disable-next-line @typescript-eslint/no-var-requires
				const VTLTools = require('@making-sense/vtl-2-0-antlr-tools-ts');
				const {
					monarchDefinition,
					getSuggestionsFromRange,
					// eslint-disable-next-line @typescript-eslint/no-var-requires
				} = require('./vtl-monaco');
				const customTools = {
					...VTLTools,
					getSuggestionsFromRange,
					monarchDefinition,
				};
				return (
					<VTLEditor
						script={script}
						tools={customTools}
						height="10vh"
						options={{ minimap: { enabled: false }, lineNumbers: false }}
					/>
				);
			}}
		</BrowserOnly>
	);
});

export default Editor;
