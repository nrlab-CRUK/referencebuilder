/*
 * Miscellaneous helper functions used all over the pipeline.
 */

def speciesPath(genomeInfo)
{
    def folder = genomeInfo['name.scientific'].toLowerCase().replace(' ', '_')
    return "${params.referenceTop}/${folder}"
}

def assemblyPath(genomeInfo)
{
    return "${speciesPath(genomeInfo)}/${genomeInfo['version']}"
}

def filenameRoot(genomeInfo)
{
    return "${genomeInfo['abbreviation']}.${genomeInfo['version']}"
}

/*
 * Get the size of a collection of things. It might be that the thing
 * passed in isn't a collection or map, in which case the size is 1.
 *
 * See https://github.com/nextflow-io/nextflow/issues/2425
 */
def sizeOf(thing)
{
    return (thing instanceof Collection || thing instanceof Map) ? thing.size() : 1
}

/**
 * Give a number for the Java heap size based on the task memory, allowing for
 * some overhead for the JVM itself from the total allowed.
 */
def javaMemMB(task)
{
    return task.memory.toMega() - 128
}
