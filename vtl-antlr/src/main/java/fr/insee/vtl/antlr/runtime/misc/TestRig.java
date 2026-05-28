/*
 * Copyright (c) 2012-2017 The ANTLR Project. All rights reserved.
 * Use of this file is governed by the BSD 3-clause license that
 * can be found in the LICENSE.txt file in the project root.
 */

package fr.insee.vtl.antlr.runtime.misc;

import java.lang.reflect.Method;

/** A proxy for the real fr.insee.vtl.antlr.gui.TestRig that we moved to tool
 *  artifact from runtime.
 *
 *  @deprecated
 *  @since 4.5.1
 */
public class TestRig {
	public static void main(String[] args) {
		try {
			Class<?> testRigClass = Class.forName("fr.insee.vtl.antlr.gui.TestRig");
			System.err.println("Warning: TestRig moved to fr.insee.vtl.antlr.gui.TestRig; calling automatically");
			try {
				Method mainMethod = testRigClass.getMethod("main", String[].class);
				mainMethod.invoke(null, (Object)args);
			}
			catch (Exception nsme) {
				System.err.println("Problems calling fr.insee.vtl.antlr.gui.TestRig.main(args)");
			}
		}
		catch (ClassNotFoundException cnfe) {
			System.err.println("Use of TestRig now requires the use of the tool jar, antlr-4.X-complete.jar");
			System.err.println("Maven users need group ID org.antlr and artifact ID antlr4");
		}
	}
}
