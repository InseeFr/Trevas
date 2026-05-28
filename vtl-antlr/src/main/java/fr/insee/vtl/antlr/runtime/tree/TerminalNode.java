/*
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 * Use of this file is governed by the BSD 3-clause license that
 * can be found in the LICENSE.txt file in the project root.
 */

package fr.insee.vtl.antlr.runtime.tree;

import fr.insee.vtl.antlr.runtime.Token;

public interface TerminalNode extends ParseTree {
	Token getSymbol();
}
