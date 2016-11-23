/**
 * Copyright (C) 2013 EURECOM (www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.eurecom.hybris.kvs.drivers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * File-based key-value backend - for testing purposes.
 * NB: different FileKvs write in the same common container (!)
 * 
 * @author P. Viotti
 */
public class FileKvs extends Kvs {

	public FileKvs(String id, String container, boolean enabled, int cost) throws IOException {
		super(id, container, enabled, cost);
		Path path = Paths.get(this.rootContainer);
		Files.createDirectories(path);
	}

	public void put(String key, byte[] value) throws IOException {
		Path path = Paths.get(this.rootContainer + File.separator + key);
		Files.write(path, value);
	}

	public byte[] get(String key) throws IOException {
		Path path = Paths.get(this.rootContainer + File.separator + key);
		return Files.readAllBytes(path);
	}

	public void delete(String key) throws IOException {
		Path path = Paths.get(this.rootContainer + File.separator + key);
		Files.delete(path);
	}

	public List<String> list() {
		String[] objs = new File(this.rootContainer).list();
		if (objs == null)
			return new ArrayList<String>();
		else
			return new ArrayList<String>(Arrays.asList(objs));
	}

}
