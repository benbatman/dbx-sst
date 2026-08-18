class PCMCaptureProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.chunkSize = 4096;
        this.buffer = new Float32Array(this.chunkSize);
        this.writeIndex = 0;
    }

    process(inputs) {
        const input = inputs[0][0];
        if (!input) return true;

        let read = 0;
        while (read < input.length) {
            const toCopy = Math.min(
                this.chunkSize - this.writeIndex,
                input.length - read,
            );
            this.buffer.set(input.subarray(read, read + toCopy), this.writeIndex);
            this.writeIndex += toCopy;
            read += toCopy;

            if (this.writeIndex === this.chunkSize) {
                this.port.postMessage(this.buffer.slice());
                this.writeIndex = 0;
            }
        }
        return true;
    }
}

registerProcessor("pcm-capture", PCMCaptureProcessor);