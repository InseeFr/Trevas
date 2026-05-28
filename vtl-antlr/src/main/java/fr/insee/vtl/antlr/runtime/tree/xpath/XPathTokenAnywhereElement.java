/*
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 * Use of this file is governed by the BSD 3-clause license that
 * can be found in the LICENSE.txt file in the project root.
 */

package fr.insee.vtl.antlr.runtime.tree.xpath;

import fr.insee.vtl.antlr.runtime.tree.ParseTree;
import fr.insee.vtl.antlr.runtime.tree.Trees;

import java.util.Collection;

public class XPathTokenAnywhereElement extends XPathElement {
	protected int tokenType;
	public XPathTokenAnywhereElement(String tokenName, int tokenType) {
		super(tokenName);
		this.tokenType = tokenType;
	}

	@Override
	public Collection<ParseTree> evaluate(ParseTree t) {
		return Trees.findAllTokenNodes(t, tokenType);
	}
}
