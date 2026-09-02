// Worklet captures every source sample and places it in 
// the shared array buffer ring

const STATE_WRITE_INDEX = 0;
const STATE_READ_INDEX = 1;
const STATE_OVERRUN_SAMPLES = 2;
const STATE_STOPPED = 3;

class PCMCaptureProcessor extends AudioWorkletProcessor {
    constructor(options) {
        super();

        const processorOptions = options.processorOptions ?? {};
        const { audioSAB, stateSAB, capacity } = processorOptions;
        
        if (
            !(audioSAB instanceof SharedArrayBuffer) ||
            !(stateSAB instanceof SharedArrayBuffer)
        ) {
            throw new Error("pcm-capture requires SharedArrayBuffer inputs")
        }

        this.audio = new Float32Array(audioSAB);
        this.state = new Int32Array(stateSAB);
        this.capacity = capacity;

        if (this.audio.length !== capacity) {
            throw new Error("pcm-capture ring capacity does not match audioSAB");
        }
    }

    process(inputs) {
        if (Atomics.load(this.state, STATE_STOPPED) !== 0) {
            return false;
        }
        const input = inputs[0]?.[0]; // first input, first channel (mono)
        if (!input || input.length === 0) return true;

        let writeIndex = Atomics.load(this.state, STATE_WRITE_INDEX);
        const readIndex = Atomics.load(this.state, STATE_READ_INDEX)


        const used = (writeIndex - readIndex + this.capacity) % this.capacity;

        // One slot remains unused so equal indices always mean "empty"
        const free = this.capacity - used - 1

        if (input.length > free) {
            Atomics.add(
                this.state, 
                STATE_OVERRUN_SAMPLES, 
                input.length
            );
            Atomics.store(this.state, STATE_STOPPED, 1)
            Atomics.notify(this.state, STATE_WRITE_INDEX, 1)
            return false;
        }

        for (let i=0; i < input.length; i++) {
            this.audio[writeIndex] = input[i];
            writeIndex++;

            if (writeIndex === this.capacity) {
                writeIndex = 0;
            }
        }

        // Publishing the write index after copying makes the samples visible to the worker before it starts consuming them
        Atomics.store(
            this.state, 
            STATE_WRITE_INDEX, 
            writeIndex
        );
        Atomics.notify(this.state, STATE_WRITE_INDEX, 1);

        return true
    }
}

registerProcessor("pcm-capture", PCMCaptureProcessor);